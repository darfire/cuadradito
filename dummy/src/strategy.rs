use async_trait::async_trait;

use base::strategy::Strategy;
use base::context::Context;
use crate::market::{DummyMarket, DummyAction};

pub struct DummyStrategy {
}


#[async_trait]
impl Strategy<DummyMarket> for DummyStrategy {
    async fn step(&self, context: Context<DummyMarket>) -> Vec<DummyAction> {
        let positions = context.positions;

        let mut actions = vec![];

        for p in positions {
            if p.n_shares == 0 {
                actions.push(DummyAction::Buy(p.symbol, 1));
            } else {
                actions.push(DummyAction::Sell(p.symbol, 1));
            }
        }

        actions
    }
}