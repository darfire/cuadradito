#![allow(unused)]
use std::sync::{Arc, RwLock};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{TimestampNanosecondType, ArrowPrimitiveType};
use arrow::datatypes::Schema;
use arrow::compute::concat_batches;
use std::collections::HashMap;
use async_trait::async_trait;
use tokio::sync::RwLock as AsyncRwLock;

use super::view::{DataLayerView, DataStreamView};


pub type Time = <TimestampNanosecondType as ArrowPrimitiveType>::Native;

pub type WrappedDataStream = Arc<AsyncRwLock<DataStream>>;

#[derive(Debug)]
pub enum DataError {
    InvalidPath(String),
    InvalidData(String),
    ArrowError(arrow::error::ArrowError),
    IOError(std::io::Error),
}

impl From<arrow::error::ArrowError> for DataError {
    fn from(e: arrow::error::ArrowError) -> DataError {
        DataError::ArrowError(e)
    }
}

impl From<std::io::Error> for DataError {
    fn from(e: std::io::Error) -> DataError {
        DataError::IOError(e)
    }
}

pub struct DataFrame {
    batch: RecordBatch,
}

impl DataFrame {
    pub fn schema(&self) -> Arc<Schema> {
        self.batch.schema()
    }
}

/// build a DataFrame from a RecordBatch
impl From<RecordBatch> for DataFrame {
    fn from(batch: RecordBatch) -> DataFrame {
        DataFrame {
            batch: batch,
        }
    }
}

impl From<DataFrame> for RecordBatch {
    fn from(data: DataFrame) -> RecordBatch {
        data.batch
    }
}

#[async_trait] 
/// A data stream driver is responsible for fetching and writing data to persistent storage
pub trait DataStreamDriver: Send + Sync {
    async fn get_data(&self, start: Option<Time>, end: Option<Time>) -> Result<DataFrame, DataError>;

    async fn write_data(&mut self, data: DataFrame) -> Result<(), DataError>; 

    fn schema(&self) -> Arc<Schema>;
}

#[async_trait]
// a driver for the data layer
pub trait DataLayerDriver: Send + Sync {
    async fn make_stream_driver(&self, path: &Vec<String>, schema: Arc<Schema>) -> Result<Box<dyn DataStreamDriver>, DataError>;

    async fn read_dirs(&self) -> Result<Arc<RwLock<DataDir<WrappedDataStream>>>, DataError>;
}

/// A data stream is a data source that can be queried for data
pub struct DataStream {
    driver: Box<dyn DataStreamDriver>,

    buffer: DataFrame,
}

impl DataStream {
    pub fn new(driver: Box<dyn DataStreamDriver>) -> DataStream {
        let schema = driver.schema();
        DataStream {
            driver: driver,
            buffer: DataFrame::from(RecordBatch::new_empty(schema)),
        }
    }

    fn schema(&self) -> Arc<Schema> {
        self.driver.schema().clone()
    }

    pub async fn get_data(&self, start: Option<Time>, end: Option<Time>) -> Result<DataFrame, DataError> {
        self.driver.get_data(start, end).await
    }

    pub async fn push(&mut self, data: DataFrame) -> Result<(), DataError> {
        let schema = self.schema();
        self.buffer = concat_batches(&schema, [&self.buffer.batch, &data.batch]).map_err(|e| {
            DataError::ArrowError(e)
        })?.into();

        Ok(())
    }
}

pub enum DataNode<T: Clone> {
    Leaf(T),
    Dir(DataDir<T>),
}

pub struct DataDir<T: Clone> {
    name: String,

    children: HashMap<String, DataNode<T>>,
}

impl<T: Clone> DataDir<T> {
    pub fn new_empty(name: String) -> DataDir<T> {
        DataDir {
            name,
            children: HashMap::new(),
        }
    }

    pub fn add_leaf(&mut self, path: &Vec<String>, node: T) -> Result<(), DataError> {
        if path.len() == 0 {
            return Err(DataError::InvalidPath("empty path".to_string()));
        }

        let name = path[0].clone();
        let path = path[1..].to_vec();

        let mut child = self.children.get_mut(&name);

        match child {
            Some(DataNode::Leaf(_)) => {
                return Err(DataError::InvalidPath("leaf already exists".to_string()));
            },
            Some(DataNode::Dir(dir)) => {
                return dir.add_leaf(&path, node);
            },
            None => {
                if path.len() == 0 {
                    self.children.insert(name, DataNode::Leaf(node));

                } else {
                    let mut dir = DataDir::new_empty(name.clone());
                    dir.add_leaf(&path, node)?;
                    self.children.insert(name, DataNode::Dir(dir));
                }
                return Ok(());
            }
        }
    }

    pub fn add_dir(&mut self, path: &Vec<String>, dir: DataDir<T>) -> Result<(), DataError> {
        if path.len() == 0 {
            return Err(DataError::InvalidPath("empty path".to_string()));
        }

        let name = path[0].clone();
        let path = path[1..].to_vec();

        let mut child = self.children.get_mut(&name);

        match child {
            Some(DataNode::Leaf(_)) => {
                return Err(DataError::InvalidPath("leaf already exists".to_string()));
            },
            Some(DataNode::Dir(child_dir)) => {
                return child_dir.add_dir(&path, dir);
            },
            None => {
                if path.len() == 0 {
                    self.children.insert(name, DataNode::Dir(dir));
                } else {
                    let mut new_dir = DataDir::new_empty(name.clone());
                    new_dir.add_dir(&path, dir)?;
                    self.children.insert(name, DataNode::Dir(new_dir));
                }
                return Ok(());
            }
        }
    }

    pub fn get_leaf(&self, path: &Vec<String>) -> Option<T> {
        if path.len() == 0 {
            return None;
        }

        let name = path[0].clone();
        let path = path[1..].to_vec();

        let child = self.children.get(&name);

        match child {
            Some(DataNode::Leaf(node)) => {
                Some(node.clone())
            },
            Some(DataNode::Dir(dir)) => {
                dir.get_leaf(&path)
            },
            None => {
                None
            },
        }
    }

    pub fn map_leafs<F: Clone, U: Clone>(&self, f: F) -> Box<DataDir<U>>
        where F: Fn(&T) -> U {
        let new_children = self.children.iter().map(|(name, node)| {
            match node {
                DataNode::Leaf(node) => {
                    (name.clone(), DataNode::Leaf(f(node)))
                },
                DataNode::Dir(dir) => {
                    (name.clone(), DataNode::Dir(*dir.map_leafs(f.clone())))
                },
            }
        }).collect();

        Box::new(DataDir {
            name: self.name.clone(),
            children: new_children,
        })
    }

    pub fn filter_leafs<F: Clone>(&self, f: F) -> Box<DataDir<T>>
        where F: Fn(&T) -> bool {
        let new_children = self.children.iter().filter_map(|(name, node)| {
            match node {
                DataNode::Leaf(node) => {
                    if f(node) {
                        Some((name.clone(), DataNode::Leaf(node.clone())))
                    } else {
                        None
                    }
                },
                DataNode::Dir(dir) => {
                    let new_dir = dir.filter_leafs(f.clone());
                    if new_dir.children.len() > 0 {
                        Some((name.clone(), DataNode::Dir(*new_dir)))
                    } else {
                        None
                    }
                },
            }
        }).collect();

        Box::new(DataDir {
            name: self.name.clone(),
            children: new_children,
        })
    }

    pub fn filter_map_leafs<F: Clone, U: Clone>(&self, f: F) -> Box<DataDir<U>>
        where F: Fn(&T) -> Option<U> {
        let new_children = self.children.iter().filter_map(|(name, node)| {
            match node {
                DataNode::Leaf(node) => {
                    f(node).map(|new_node| {
                        (name.clone(), DataNode::Leaf(new_node))
                    })
                },
                DataNode::Dir(dir) => {
                    let new_dir = dir.filter_map_leafs(f.clone());
                    if new_dir.children.len() > 0 {
                        Some((name.clone(), DataNode::Dir(*new_dir)))
                    } else {
                        None
                    }
                },
            }
        }).collect();

        Box::new(DataDir {
            name: self.name.clone(),
            children: new_children,
        })
    }
}

pub enum DataChild<Node: Clone> {
    Leaf(Arc<Node>),
    Dir(Arc<DataDir<Node>>),
}

/// It abstracts the way data is stored and retrieved
pub struct DataLayer {
    driver: Box<dyn DataLayerDriver>,

    root_dir: Arc<RwLock<DataDir<WrappedDataStream>>>,
}

impl DataLayer {
    pub fn new_empty(driver: Box<dyn DataLayerDriver>) -> DataLayer {
        DataLayer {
            driver: driver,
            root_dir: Arc::new(RwLock::new(DataDir::new_empty(".".to_string()))),
        }
    }

    pub fn new(driver: Box<dyn DataLayerDriver>, root_dir: Arc<RwLock<DataDir<WrappedDataStream>>>) -> DataLayer {
        DataLayer {
            driver: driver,
            root_dir,
        }
    }

    pub fn get_stream(&self, path: &Vec<String>) -> Option<WrappedDataStream> {
        self.root_dir.read().unwrap().get_leaf(path)
    }

    pub fn get_view(&self, start: Option<Time>, end: Option<Time>) -> DataLayerView {
        let root_dir = self.root_dir.read().unwrap().map_leafs(|stream| {
            Arc::new(DataStreamView::new(stream.clone(), start, end))
        });

        DataLayerView::new(root_dir.into(), start, end)
    }

    pub fn get_filtered_view(&self, start: Option<Time>, end: Option<Time>,
        filter: fn(WrappedDataStream) -> bool) -> DataLayerView {

        let root_dir = self.root_dir.read().unwrap().filter_map_leafs(|stream| {
            if filter(stream.clone()) {
                Some(Arc::new(DataStreamView::new(stream.clone(), start, end)))
            } else {
                None
            }
        });

        DataLayerView::new(root_dir.into(), start, end)
    }

    pub async fn ensure_stream(&self, path: &Vec<String>, schema: Arc<Schema>) -> Result<WrappedDataStream, DataError> {
        if let Some(stream) = self.root_dir.read().unwrap().get_leaf(path) {
            return Ok(stream);
        }

        let driver = self.driver.make_stream_driver(path, schema.clone()).await?;

        let stream = Arc::new(AsyncRwLock::new(DataStream::new(driver)));

        self.root_dir.write().unwrap().add_leaf(path, stream.clone());

        Ok(stream)
    }

    pub async fn push(&self, path: &Vec<String>, data: DataFrame) -> Result<(), DataError> {
        let stream = self.ensure_stream(path, data.schema()).await?;

        stream.write().await.push(data).await?;

        Ok(())
    }
}