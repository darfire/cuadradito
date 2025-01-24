use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutext;

use crate::data::DataLayerView;
use crate::market::Market;

/// The context that is passed to a Strategy
pub struct Context<M: Market> {
    pub data: DataLayerView,
    pub market: Arc<AsyncMutext<M>>,
    pub positions: Vec<M::Position>,
    pub current_time: u64,
}