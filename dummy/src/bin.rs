use std::sync::Arc;

use base::time::{set_time_keeper, GlobalTimeKeeper, SystemTimeKeeper};
use base::data::DataLayer;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinSet;

use dummy::market::DummyMarket;
use dummy::market::DummyMarketBackend;
use dummy::data::DummyDataLayerDriver;
use dummy::execute::Executor;
use dummy::strategy::DummyStrategy;


#[tokio::main]
async fn main() {
    let symbols = ["AAPL", "GOOGL", "MSFT", "AMZN"];

    let symbols = symbols.iter().map(|s| s.to_string()).collect::<Vec<String>>();

    let backend = Arc::new(AsyncMutex::new(DummyMarketBackend::new(symbols)));

    let market = Arc::new(AsyncMutex::new(DummyMarket::with_backend(backend.clone())));

    let strategy = Arc::new(AsyncMutex::new(DummyStrategy {}));

    let driver = Box::new(DummyDataLayerDriver::new());

    let data_layer: Arc<DataLayer> = Arc::new(DataLayer::new_empty(driver));

    let time_keeper = SystemTimeKeeper::new();

    set_time_keeper(Box::new(GlobalTimeKeeper::System(time_keeper)));

    let mut join_set = JoinSet::new();

    let notify = Arc::new(Notify::new());

    let notify2 = notify.clone();

    join_set.spawn(async move {
        backend.lock().await.run(notify2).await;
    });

    let executor = Executor::new(market, strategy, data_layer).with_interval(1);

    let notify3 = notify.clone();

    join_set.spawn(async move {
        executor.run(notify3).await;
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl-C");
        }
        // other termination conditions
    }

    notify.notify_waiters();

    join_set.join_all().await;
}