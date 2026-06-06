# High-Performance Limit Order Book Engine

A low-latency limit order book engine written in C++20, implementing price-time priority matching — the same algorithm used by real exchanges.

## Benchmark (Apple M1)
- **5,069,628 orders/second**
- **132ns average latency**
- ~519,000 trades generated from 1,000,000 mixed orders in 197ms

## Features
- Price-time priority matching engine (FIFO within price levels)
- Integer tick-based pricing — no floating point drift
- Template half-books: bids (`std::greater`) and asks (`std::less`)
- Lock-free atomic order IDs via `std::atomic` with `memory_order_relaxed`
- Three order types: `LIMIT`, `MARKET`, `IOC` (Immediate-Or-Cancel)
- Nanosecond timestamps via `std::chrono::steady_clock`
- Market depth snapshots, trade history, execution reports

## Build & Run
```bash
g++ -O3 -std=c++20 -march=native -o orderbook orderbook.cpp
./orderbook
```

## Architecture
- **HalfBook<Compare>** — templated price level map, O(log n) best price access
- **PriceLevel** — intrusive FIFO list of resting orders at a single price
- **OrderBook** — matching engine coordinating both half-books
- Integer prices stored as ticks (e.g. `18500` = `$185.00`)
