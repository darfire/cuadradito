use std::collections::HashMap;
use std::sync::Arc;
use crate::data::{DataFrame, DataLayer};


pub trait MarketBackend {
}


pub trait MarketSimulator: MarketBackend {
    fn needs_data_layer(&self) -> bool;

    fn set_data_layer(&self, data_layer: Arc<DataLayer>);

    fn set_current_time(&self, time: u64);
}


pub trait MarketSchedule {
    fn can_trade(&self, time: u64) -> bool;

    fn get_next_trade_time(&self, time: u64) -> u64;
}


/// Abstracts a specific Market
/// To define a Market you need to define
/// - The data it produces
/// - The actions it can take
/// - The positions it can hold
/// - A market-specific Portfolio
#[allow(async_fn_in_trait)]
pub trait Market {
    type Action;
    type Position;

    /// Get the data for a specific time range
    async fn get_data(&self, start: u64, end: u64) -> HashMap<String, &DataFrame>;

    /// Perform a list of actions
    async fn execute_actions(&self, actions: Vec<Self::Action>);

    /// Get the current positions
    async fn get_positions(&self) -> Vec<Self::Position>;
}