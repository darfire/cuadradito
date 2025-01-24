use std::sync::Arc;
use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use rand_distr::{Distribution, Normal};
use rand::{self, SeedableRng};
use futures::future::FutureExt;

use tokio::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

use arrow::array::{ArrayRef, ArrowPrimitiveType, PrimitiveBuilder, ArrayBuilder};
use arrow::datatypes::{Float64Type, TimestampNanosecondType};

use base::market::{MarketBackend, Market};
use base::data::DataFrame;
use base::time::{get_current_time, sleep};


#[derive(Clone)]
pub enum DummyAction {
  Buy(String, i64),
  Sell(String, i64),
}

#[allow(unused)]
pub struct DummyPosition {
  pub symbol: String,
  pub n_shares: i64,
  pub price: f64,
}

pub struct ArrayBucket<T: ArrowPrimitiveType> {
  arrays: Vec<ArrayRef>,
  builder: PrimitiveBuilder<T>,
  chunk_size: usize,  
}

impl<T: ArrowPrimitiveType> ArrayBucket<T> {
  pub fn new(chunk_size: usize) -> ArrayBucket<T> {
    let builder = PrimitiveBuilder::<T>::new();

    ArrayBucket {
      arrays: vec![],
      builder,
      chunk_size,
    }
  }

  pub fn new_with_values(values: Vec<T::Native>) -> ArrayBucket<T> {
    let mut builder = PrimitiveBuilder::<T>::new();

    let is_valid = values.iter().map(|_| true).collect::<Vec<bool>>();

    builder.append_values(&values, &is_valid);

    let array = builder.finish();

    ArrayBucket {
      arrays: vec![Arc::new(array)],
      builder: PrimitiveBuilder::<T>::new(),
      chunk_size: 1024,
    }
  }

  pub fn push(&mut self, value: T::Native) {
    self.builder.append_value(value);

    if self.builder.len() >= self.chunk_size {
      let new_array = self.builder.finish();

      self.arrays.push(Arc::new(new_array));

      self.builder = PrimitiveBuilder::<T>::new();
    }
  }

  pub fn len(&self) -> usize {
    self.builder.len()
  }

  pub fn get_last_value(&self) -> Option<T::Native> {
    if self.builder.len() == 0 {
      return None;
    }

    self.builder.as_any().downcast_ref::<PrimitiveBuilder<T>>().unwrap().values_slice().last().copied()
  }
}

pub struct DummyMarketBackend {
  time: ArrayBucket<TimestampNanosecondType>,
  prices: HashMap<String, ArrayBucket<Float64Type>>,
  shares: HashMap<String, i64>,
  update_every: f64,
  std_dev: f64,
  cash: f64,
  rng: rand::rngs::StdRng,
  return_distribution: Normal<f64>,
}

impl DummyMarketBackend {
  pub fn new(symbols: Vec<String>) -> DummyMarketBackend {
    let time = ArrayBucket::new(0);
    let prices = symbols.iter().map(|s| (s.clone(), ArrayBucket::new_with_values(vec![100 as f64]))).collect();
    let shares = symbols.iter().map(|s| (s.clone(), 0)).collect();

    DummyMarketBackend {
      time,
      prices,
      shares,
      update_every: 1.0,
      std_dev: 5.0,
      cash: 1000.0,
      rng: rand::rngs::StdRng::seed_from_u64(42),
      return_distribution: Normal::new(1.0, 0.1).expect("Invalid distribution parameters"),
    }
  }

  pub fn with_update_every(&mut self, update_every: f64) -> &mut Self {
    self.update_every = update_every;
    self
  }

  pub fn with_std_dev(&mut self, std_dev: f64) -> &mut Self {
    self.std_dev = std_dev;
    self.return_distribution = Normal::new(1.0, std_dev).expect("Invalid distribution parameters");
    self
  }

  pub fn with_cash(&mut self, cash: f64) -> &mut Self {
    self.cash = cash;
    self
  }

  pub async fn run(&mut self, notify: Arc<tokio::sync::Notify>) {
    loop {
      if let Some(_) = notify.notified().now_or_never() {
        break;
      }

      sleep(Duration::from_secs_f64(self.update_every)).await;

      let now= get_current_time().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;

      self.time.push(now);

      for (_, price) in self.prices.iter_mut() {
        let r = self.return_distribution.sample(&mut self.rng);
        let last_value = price.get_last_value().unwrap();
        price.push(last_value * r);
      }
    }
  }

  async fn get_positions(&self) -> Vec<DummyPosition> {
    let positions = self.shares.iter().map(|(symbol, n_shares)| {
      let price_bucket = self.prices.get(symbol).unwrap();
      DummyPosition {
        symbol: symbol.clone(),
        n_shares: *n_shares,
        price: price_bucket.get_last_value().unwrap(),
      }
    }).collect();

    positions
  }

  async fn execute(&mut self, actions: Vec<DummyAction>) {
    for a in actions {
      self.execute_action(a);
    }
  }

  fn execute_action(&mut self, action: DummyAction) {
    let symbol= match action {
      DummyAction::Buy(ref symbol, _) => symbol,
      DummyAction::Sell(ref symbol, _) => symbol,
    };

    let price_bucket = self.prices.get(symbol).unwrap();

    let price = price_bucket.get_last_value().unwrap();

    match action {
      DummyAction::Buy(ref symbol, n_shares) => {
        let cost = n_shares as f64 * price;

        if cost > self.cash {
          return;
        }

        let current_shares = *self.shares.get(symbol).unwrap();

        self.shares.insert(symbol.clone(), current_shares + n_shares);

        self.cash -= cost;
      },
      DummyAction::Sell(ref symbol, n_shares) => {
        let gain = n_shares as f64 * price;

        let current_shares = *self.shares.get(symbol).unwrap();

        if n_shares > current_shares {
          return;
        }

        self.shares.insert(symbol.clone(), current_shares - n_shares);

        self.cash += gain;
      },
    }
  }
}

impl MarketBackend for DummyMarketBackend {
}

pub struct DummyMarket {
  backend: Arc<AsyncMutex<DummyMarketBackend>>,
}

impl DummyMarket {
  pub fn with_backend(backend: Arc<AsyncMutex<DummyMarketBackend>>) -> DummyMarket {
    DummyMarket {
      backend: backend,
    }
  }
}

impl Market for DummyMarket {
  type Action = DummyAction;
  type Position = DummyPosition;

  async fn get_data(&self, _start: u64, _end: u64) -> HashMap<String, &DataFrame> {
    HashMap::new()
  }

  async fn execute_actions(&self, actions: Vec<Self::Action>) {
    self.backend.lock().await.execute(actions).await
  }

  async fn get_positions(&self) -> Vec<Self::Position> {
    let read_backend = self.backend.lock().await;
    read_backend.get_positions().await
  }
}


impl DummyMarket {
  pub fn connect_backend(&mut self, backend: Arc<AsyncMutex<DummyMarketBackend>>) {
    self.backend = backend.clone();
  }
}