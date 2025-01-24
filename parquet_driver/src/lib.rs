#![allow(unused)]
use std::sync::{Arc, RwLock};
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use std::io::Error as IOError;
use arrow::buffer;
use arrow::compute::concat_batches;
use async_trait::async_trait;
use parquet::schema;
use tokio::sync::RwLock as AsyncRwLock;

use futures::TryStreamExt;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema};
use arrow::array::{Int64Array};

use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::statistics::Statistics;

use base::data::*;


struct DataChunk {
  schema: Arc<Schema>,
  start: Time,
  end: Time,
  path: PathBuf,
}


/// A DataStreamDriver backed by parquet files
pub struct ParquetDataStreamDriver {
  /// the directory where the data chunks are stored
  path: PathBuf,

  /// the data chunks that make up this stream
  chunks: Vec<DataChunk>,

  /// the data that we are holding in memory, either from the data chunks or from pushes
  buffer: Vec<RecordBatch>,

  /// the schema of the data
  schema: Arc<Schema>,
}

impl ParquetDataStreamDriver {
    async fn new(path: PathBuf) -> Result<ParquetDataStreamDriver, DataError> {
      let mut chunks: Vec<DataChunk> = vec![];
      for entry in path.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        let fname = path.file_name().unwrap().to_string_lossy().to_string();

        if path.is_file() && fname.ends_with(".parquet") && fname.starts_with("C:") {
          let chunk = DataChunk::new(&path).await?;
          chunks.push(chunk);
        };
      }

      if chunks.len() == 0 {
        return Err(DataError::InvalidPath("no parquet files found".to_string()));
      }

      chunks.sort_by(|a, b| a.end.cmp(&b.end));

      let schema = chunks[0].schema.clone();

      let mut last_end  = chunks[0].end;

      for chunk in &chunks[1..] {
        if chunk.schema != schema {
          return Err(DataError::InvalidData("schema mismatch".to_string()));
        }

        if chunk.start < last_end {
          return Err(DataError::InvalidData("chunks overlap".to_string()));  
        }
      }

      Ok(ParquetDataStreamDriver {
        schema,
        chunks,
        path,
        buffer: vec![],
      })
    }
}

impl DataChunk {
  async fn new(path: &PathBuf) -> Result<DataChunk, DataError> {
    if !path.is_file() {
      return Err(DataError::InvalidPath("not a file".to_string()));
    }

    let file = tokio::fs::File::open(path).await?;

    let builder = ParquetRecordBatchStreamBuilder::new(file).await
      .or_else(|e| Err(DataError::InvalidData(e.to_string())))?;

    let metadata = builder.metadata();

    let file_metadata = metadata.file_metadata();

    let schema = parquet_to_arrow_schema(
      file_metadata.schema_descr(), None)
        .or_else(|e| Err(DataError::InvalidData(e.to_string())))?.into();

    let first_stats = metadata.row_groups()[0].column(0)
      .statistics().ok_or(DataError::InvalidData("No statistics found".to_string()))?;

    let last_stats = metadata.row_groups().last().unwrap().column(0)
      .statistics().ok_or(DataError::InvalidData("No statistics found".to_string()))?;

    let start = match first_stats {
      Statistics::Int64(stats) => *stats.min_opt().ok_or(DataError::InvalidData("No min statistics found".to_string()))?,
      Statistics::Int32(stats) => i64::from(*stats.min_opt().ok_or(DataError::InvalidData("No min statistics found".to_string()))?),
      _ => return Err(DataError::InvalidData("invalid statistics".to_string())),
    };

    let end = match last_stats {
      Statistics::Int64(stats) => *stats.max_opt().ok_or(DataError::InvalidData("No max statistics found".to_string()))?,
      Statistics::Int32(stats) => i64::from(*stats.max_opt().ok_or(DataError::InvalidData("No max statistics found".to_string()))?),
      _ => return Err(DataError::InvalidData("invalid statistics".to_string())),
    };

    let start = Time::from(start);

    let end = Time::from(end);

    Ok(DataChunk {
      schema,
      start,
      end,
      path: path.clone(),
    })
  }

  async fn read_data(&self, start: Time, end: Time) -> Result<Vec<RecordBatch>, DataError> {
    let file = tokio::fs::File::open(&self.path).await?;

    let builder = ParquetRecordBatchStreamBuilder::new(file).await
      .or_else(|e| Err(DataError::InvalidData(e.to_string())))?;

    let stream = builder.build()
      .or_else(|e| Err(DataError::InvalidData(e.to_string())));

    let mut batches = vec![];

    let mut stream = stream.unwrap();

    while let Some(batch) = stream.try_next().await.or_else(|e| Err(DataError::InvalidData(e.to_string())))? {
      let (batch_start, batch_end) = batch_time_range(&batch);

      let intersection = ranges_intersect((Some(start), Some(end)), (batch_start, batch_end));

      if let Some((start, end)) = intersection {
        let batch = batch_subset(&batch, start, end);
        batches.push(batch);
      }
    }

    Ok(batches)
  }
}


/// A DataLayerDriver backed by parquet files
pub struct ParquetDataLayerDriver {
  path: PathBuf,
}

async fn read_dir(path: &Path) -> Result<DataDir<WrappedDataStream>, DataError> {
  let mut result = DataDir::new_empty(".".to_string());

  let mut queue = VecDeque::new();

  let path_buf = PathBuf::from(path); 

  queue.push_back(path_buf);

  let mut tail_path = PathBuf::new();

  while let Some(path) = queue.pop_front() {
    let mut dir = DataDir::new_empty(path.file_name().unwrap().to_string_lossy().to_string());

    for entry in path.read_dir()? {
      let entry = entry?;
      let path = entry.path();

      let fname = path.file_name().unwrap().to_string_lossy().to_string();

      if path.is_dir() {
        if path.starts_with("D:") {
          queue.push_back(path.clone());
        } else if path.starts_with("L:") {
          let stream_driver = ParquetDataStreamDriver::new(path).await?;
          let stream = Arc::new(AsyncRwLock::new(DataStream::new(Box::new(stream_driver))));
          dir.add_leaf(&vec![fname], stream)?;
        } 
      }
    }

    let path_tokens = path.iter().map(|x| x.to_str().unwrap().to_string()).collect::<Vec<String>>();

    result.add_dir(&path_tokens, dir)?;
  }

  Ok(result)
}

impl ParquetDataLayerDriver {
    fn new(root_dir: String) -> ParquetDataLayerDriver {
        ParquetDataLayerDriver {
          path: PathBuf::from(root_dir),
        }
    }
}


#[async_trait]
impl DataLayerDriver for ParquetDataLayerDriver {
    async fn read_dirs(&self) -> Result<Arc<RwLock<DataDir<WrappedDataStream>>>, DataError> {
      let ret = read_dir(&self.path).await?;

      Ok(Arc::new(RwLock::new(ret)))
    }

    async fn make_stream_driver(&self, path: &Vec<String>, spec: Arc<Schema>) -> Result<Box<dyn DataStreamDriver>, DataError> {
      let path = self.path.iter().collect::<PathBuf>();
      let parent = path.parent().ok_or(DataError::InvalidPath("not enough path elements".to_string()))?;

      self.ensure_dir(parent);

      let driver = ParquetDataStreamDriver::new(path).await?;

      Ok(Box::new(driver))
    }
}

impl ParquetDataLayerDriver {
    fn ensure_dir(&self, path: &Path) -> Result<(), IOError> {
        let full_path = self.path.join(path);  
        if !full_path.exists() {
            std::fs::create_dir_all(full_path)?;
        }

        Ok(())
    }
}


fn ranges_intersect(a: (Option<Time>, Option<Time>), b: (Time, Time)) -> Option<(Time, Time)> {
  let (a_start, a_end) = a;
  let (b_start, b_end) = b;

  if let Some(a_start) = a_start {
    if a_start > b_end {
      return None;
    }
  }

  if let Some(a_end) = a_end {
    if a_end < b_start {
      return None;
    }
  }

  Some((a_start.unwrap_or(b_start), a_end.unwrap_or(b_end)))
}


fn batch_time_range(batch: &RecordBatch) -> (Time, Time) {
  let start = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
  let end = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(batch.num_rows() - 1);

  (Time::from(start), Time::from(end))
}

fn batch_subset(batch: &RecordBatch, start: Time, end: Time) -> RecordBatch {
  let start = start;
  let end = end;

  let start_idx = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().iter().position(|x| x == Some(start)).unwrap();
  let end_idx = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().iter().position(|x| x == Some(end)).unwrap();

  batch.slice(start_idx, end_idx - start_idx + 1)
}


#[async_trait]
impl DataStreamDriver for ParquetDataStreamDriver {
    async fn get_data(&self, start: Option<Time>, end: Option<Time>) -> Result<DataFrame, DataError> {
      let mut batches: Vec<Vec<RecordBatch>> = vec![];

      for chunk in &self.chunks {
        let intersection = ranges_intersect((start, end), (chunk.start, chunk.end));

        if let Some((start, end)) = intersection {
          let chunk_batches = chunk.read_data(start, end).await?;
          batches.push(chunk_batches);
        }
      }

      let mut batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();

      let buffer_batches: Vec<RecordBatch> = self.buffer.iter().filter_map(|x| {
        let (batch_start, batch_end) = batch_time_range(x);

        let intersection = ranges_intersect((start, end), (batch_start, batch_end));

        if let Some((start, end)) = intersection {
          Some(batch_subset(x, start, end))
        } else {
          None
        }
      }).collect();

      batches.extend_from_slice(&buffer_batches);

      let schema = self.schema.clone();

      let batch = concat_batches(&schema, &batches)?;

      Ok(DataFrame::from(batch))
    }

    async fn write_data(&mut self, data: DataFrame) -> Result<(), DataError> {
      self.buffer.push(data.into());

      if (self.should_sync()) {
        self.sync().await?;
      }

      Ok(())
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl ParquetDataStreamDriver {
    fn should_sync(&self) -> bool {
        self.buffer.len() > 100
    }

    async fn sync(&mut self) -> Result<(), DataError> {
      let start = self.buffer[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
      let end = self.buffer.last().unwrap().column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);

      let path = self.path.join(format!("C:{}-{}.parquet", start, end));

      let file = tokio::fs::File::create(path.clone()).await?;

      let mut writer = AsyncArrowWriter::try_new(file, self.schema.clone(), None)
        .or_else(|e| Err(DataError::InvalidData(e.to_string())))?;

      for batch in &self.buffer {
        writer.write(&batch).await.or_else(|e| Err(DataError::InvalidData(e.to_string())))?;
      }

      writer.close().await.or_else(|e| Err(DataError::InvalidData(e.to_string())))?;

      self.buffer.clear();

      let chunk = DataChunk {
        schema: self.schema.clone(),
        start: Time::from(start),
        end: Time::from(end),
        path,
      };

      self.chunks.push(chunk);

      Ok(())
    }
}