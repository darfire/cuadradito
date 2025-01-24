use std::sync::Arc;

use tokio::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use futures::future::FutureExt;

use base::data::DataLayer;
use base::context::Context;
use base::market::Market;
use base::strategy::Strategy;

use crate::market::DummyMarket;
use crate::strategy::DummyStrategy;


pub struct Executor {
  market: Arc<AsyncMutex<DummyMarket>>,
  strategy: Arc<AsyncMutex<DummyStrategy>>,
  data_layer: Arc<DataLayer>,
  interval: u64,
}


impl Executor {
  pub fn new(market: Arc<AsyncMutex<DummyMarket>>, strategy: Arc<AsyncMutex<DummyStrategy>>, data_layer: Arc<DataLayer>) -> Self {
    Self {
      market,
      strategy,
      data_layer,
      interval: 1,
    }
  }

  pub fn with_interval(mut self, interval: u64) -> Self {
    self.interval = interval;
    self
  }

  pub async fn run(&self, notify: Arc<tokio::sync::Notify>) {
    loop {
      if let Some(_) = notify.notified().now_or_never() {
        break;
      };

      let market = self.market.lock().await;
      let strategy = self.strategy.lock().await;

      let data_viewer = self.data_layer.get_view(None, None);

      let positions = market.get_positions().await;

      let context = Context {
        data: data_viewer,
        market: self.market.clone(),
        positions,
        current_time: 0,
      };

      let actions = strategy.step(context).await;

      market.execute_actions(actions).await;

      tokio::time::sleep(Duration::from_secs(self.interval)).await;
    }
  }
}