use crate::context::Context;
use crate::market::Market;

use async_trait::async_trait;

#[async_trait]
/// A Strategy is a set of rules that determine how to trade based on data
pub trait Strategy<M: Market> {
    async fn step(&self, context: Context<M>) -> Vec<M::Action>;
}