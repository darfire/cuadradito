use super::layer::*;
use std::sync::Arc;

pub struct DataStreamView {
    stream: WrappedDataStream,

    start: Option<Time>,
    end: Option<Time>,
}


impl DataStreamView {
    pub fn new(stream: WrappedDataStream, start: Option<Time>, end: Option<Time>) -> DataStreamView {
        DataStreamView {
            stream: stream,
            start: start,
            end: end,
        }
    }

    pub fn get_subset(&self, start: Option<Time>, end: Option<Time>) -> Arc<DataStreamView> {
        let start_array = [self.start, start];
        let end_array = [self.end, end];

        let start = start_array.iter().filter_map(|x| *x).max();
        let end = end_array.iter().filter_map(|x| *x).min();

        DataStreamView {
            stream: self.stream.clone(),
            start: start,
            end: end,
        }.into()
    }

    pub fn get_range(&self) -> (Option<Time>, Option<Time>) {
        (self.start, self.end)
    }

    pub async fn get_data(&self) -> Result<DataFrame, DataError> {
        self.stream.read().await.get_data(self.start, self.end).await
    }
}


pub struct DataLayerView {
    // we don't need to lock the root dir because it's immutable
    root_dir: Arc<DataDir<Arc<DataStreamView>>>,

    start: Option<Time>,
    end: Option<Time>,
}

impl DataLayerView {
    pub fn new(root_dir: Arc<DataDir<Arc<DataStreamView>>>, start: Option<Time>, end: Option<Time>) -> DataLayerView {
        DataLayerView {
            root_dir: root_dir,
            start: start,
            end: end,
        }
    }

    pub fn get_subset(&self, start: Option<Time>, end: Option<Time>) -> DataLayerView {
        let start_array = [self.start, start];
        let end_array = [self.end, end];

        let start = start_array.iter().filter_map(|x| *x).max();
        let end = end_array.iter().filter_map(|x| *x).min();

        DataLayerView {
            root_dir: self.root_dir.clone(),
            start: start,
            end: end,
        }
    }

    pub fn get_range(&self) -> (Option<Time>, Option<Time>) {
        (self.start, self.end)
    }
}