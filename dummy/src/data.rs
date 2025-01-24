use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use arrow::datatypes::Schema;

use base::data::{DataError, DataFrame, DataLayerDriver, DataStreamDriver, Time, WrappedDataStream, DataDir} ;

pub struct DummyDataStreamDriver {
}

pub struct DummyDataLayerDriver {
}

#[async_trait]
impl DataStreamDriver for DummyDataStreamDriver {
  async fn get_data(&self, _start: Option<Time>, _end: Option<Time>) -> Result<DataFrame, DataError> {
    unimplemented!()
  }

  async fn write_data(&mut self, _data: DataFrame) -> Result<(), DataError> {
    unimplemented!()
  }

  fn schema(&self) -> Arc<Schema> {
    unimplemented!()
  }
}

#[async_trait]
impl DataLayerDriver for DummyDataLayerDriver {
  async fn make_stream_driver(&self, _path: &Vec<String>, _schema: Arc<Schema>) -> Result<Box<dyn DataStreamDriver>, DataError> {
    Ok(Box::new(DummyDataStreamDriver {}))
  }

  async fn read_dirs(&self) -> Result<Arc<RwLock<DataDir<WrappedDataStream>>>, DataError> {
    unimplemented!()
  }
}

impl DummyDataLayerDriver {
  pub fn new() -> DummyDataLayerDriver {
    DummyDataLayerDriver {}
  }
}