# Quadradito
A high performance algorithmic trading framework, supporting both live/paper trading and backtesting.

[!WARNING]
This project is in the early stages of development and is not yet ready for production use. The API is subject to change.

[!CAUTION]
Quadradito is provided as-is, without any warranty. Use at your own risk.

## Goals
### High Performance
Quadradito is implemented in Rust + Tokio, which allows for high performance and maximizes the utilization of modern hardware.

We have also chosen Apache Arrow as the data interchange format, which is a high performance in-memory columnar data structure, interopable with many other data processing libraries.

### Generic
Quadradito takes a generic view about trading: a strategy sees the world as a set of time series. It applies its internal logic and generates a list of market-specific Actions. These, in turn, translate into market-specific Positions.

### Flexibility
Using Rust's powerful trait system, we designed Quadradito to be flexible and extensible. It is meant to be adaptable to various markets and trading strategies.

We aim to support a wide range of strategies, including but not limited to:
* Classic technical analysis
* Machine learning-based strategies
* Reinforcement learning-based strategies

### Batteries Included
We aim to provide a comprehensive set of tools for developing, testing, iterating and finally deploying and monitoring trading strategies.

## Architecture
Quadradito is designed as a collection of utilities and traits that can be used to build a trading strategy.

### Market
A `Market` is both a source of data and a place to execute trades. It provides a way to access historical data, live data, and execute trades.

Each supported exchange will be distributed as a separate crate.

A Market has a number of components:
* a MarketFrontend, which is the API that the strategy interacts with
* one or more MarketBackends, which are responsible for fetching data and executing trades

For example, an exchange can have 3 MarketBackends implemented:
* one for live trading
* one for paper trading
* one for backtesting

Finally, a Market defines:
* interfaces  for fetching historical and live data
* Actions and Positions that are specific to that market

### Strategy
A Strategy is a struct that implements the `Strategy` trait. It is specific for a given market and defines the logic that generates Actions based on the data in the `DataLayer`.

Inside it can call external libraries, run machine learning models or execute a reinforcement learning policy. The Market Actions are then transmitted to the Market.

### DataLayer
The `DataLayer`: is responsible for persisting historical data, adding live data, doing data transformations, and providing data to the Strategy.

### Executor
The `Executor` is responsible for connecting the Strategy to the Market and the DataLayer. It is Market and Strategy specific.

In the end, the result of putting all the pieces together is a Rust executable that use Tokio's async to efficiently execute the strategy, be it in live trading, paper trading, or backtesting.

## Roadmap

### Version 0.2.0
- [x] Base traits: Market, Strategy, DataLayer, DataLayerView
- [x] Dummy Market, Strategy, Executor
- [x] Parquet-backed DataLayer
- [x] TimeKeeper & ControllableTimeKeeper
- [ ] Data tools
- [ ] DataProcessor
- [ ] Example dummy executor

### Version 0.3.0
- [ ] `Market` for a real exchange (TBD)
- [ ] Technical analysis utilities
- [ ] Moving average strategy
- [ ] Monitoring infrastructure

## Contributing
Please use the issue tracker to report bugs or suggest features. Pull requests are welcome.

There are many ways to contribute to Quadradito:
* Write a new Market
* Contribute a Strategy
* Add data processing utilities
* Improve the documentation

## License
The project is licensed under the MIT license.