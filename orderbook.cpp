/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║         HIGH-PERFORMANCE LIMIT ORDER BOOK ENGINE            ║
 * ║                                                              ║
 * ║  Price-time priority matching · Lock-free order IDs         ║
 * ║  Nanosecond timestamps · Cache-aware data structures        ║
 * ║  Market depth · Trade history · Throughput benchmark        ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 * Compile:
 *   g++ -O3 -std=c++20 -march=native -o orderbook orderbook.cpp
 *
 * Run:
 *   ./orderbook
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <format>
#include <iomanip>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

// ─────────────────────────────────────────────────────────────────────────────
//  Nanosecond clock
// ─────────────────────────────────────────────────────────────────────────────
using Clock     = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;
using Nanos     = std::chrono::nanoseconds;

inline int64_t now_ns() {
    return std::chrono::duration_cast<Nanos>(
               Clock::now().time_since_epoch())
        .count();
}

// ─────────────────────────────────────────────────────────────────────────────
//  Domain types  (price in integer ticks to avoid float drift)
// ─────────────────────────────────────────────────────────────────────────────
using OrderId  = uint64_t;
using Price    = int64_t;   // price × 100  (e.g. 10050 = $100.50)
using Quantity = uint32_t;

constexpr Price TICK       = 1;          // 1 cent
constexpr Price NULL_PRICE = 0;

enum class Side : uint8_t { BUY = 0, SELL = 1 };
enum class OrderType : uint8_t { LIMIT, MARKET, IOC };   // IOC = immediate-or-cancel
enum class OrderStatus : uint8_t { OPEN, PARTIAL, FILLED, CANCELLED };

// ─────────────────────────────────────────────────────────────────────────────
//  Order
// ─────────────────────────────────────────────────────────────────────────────
struct Order {
    OrderId    id;
    Side       side;
    OrderType  type;
    OrderStatus status;
    Price      price;       // limit price (0 for MARKET)
    Quantity   qty;         // original quantity
    Quantity   leaves_qty;  // remaining (unfilled) quantity
    int64_t    timestamp;   // nanoseconds since epoch
    std::string symbol;

    Order(OrderId id, Side side, OrderType type, Price price,
          Quantity qty, const std::string& sym)
        : id{id}, side{side}, type{type},
          status{OrderStatus::OPEN},
          price{price}, qty{qty}, leaves_qty{qty},
          timestamp{now_ns()}, symbol{sym} {}

    [[nodiscard]] bool is_active() const {
        return status == OrderStatus::OPEN ||
               status == OrderStatus::PARTIAL;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
//  Trade (execution report)
// ─────────────────────────────────────────────────────────────────────────────
struct Trade {
    uint64_t  trade_id;
    OrderId   aggressive_id;   // the incoming order
    OrderId   passive_id;      // the resting order
    Price     exec_price;
    Quantity  exec_qty;
    Side      aggressor_side;
    int64_t   timestamp;
};

// ─────────────────────────────────────────────────────────────────────────────
//  PriceLevel — all orders resting at a single price (FIFO queue)
// ─────────────────────────────────────────────────────────────────────────────
struct PriceLevel {
    Price    price;
    Quantity total_qty{0};

    // Intrusive list of orders; front = oldest (matched first)
    std::list<Order*> orders;

    explicit PriceLevel(Price p) : price{p} {}

    void add(Order* o) {
        orders.push_back(o);
        total_qty += o->leaves_qty;
    }

    void remove(Order* o) {
        total_qty -= o->leaves_qty;
        orders.remove(o);
    }

    [[nodiscard]] bool empty() const { return orders.empty(); }
};

// ─────────────────────────────────────────────────────────────────────────────
//  Half-book (one side: bids or asks)
// ─────────────────────────────────────────────────────────────────────────────
//  BID side: highest price first  → std::map with std::greater
//  ASK side: lowest  price first  → std::map default
// ─────────────────────────────────────────────────────────────────────────────
template <typename Compare>
class HalfBook {
public:
    // price → PriceLevel
    std::map<Price, PriceLevel, Compare> levels;

    void add_order(Order* o) {
        auto it = levels.find(o->price);
        if (it == levels.end()) {
            auto [ins, _] = levels.emplace(o->price, PriceLevel{o->price});
            ins->second.add(o);
        } else {
            it->second.add(o);
        }
    }

    void remove_order(Order* o) {
        auto it = levels.find(o->price);
        if (it != levels.end()) {
            it->second.remove(o);
            if (it->second.empty()) levels.erase(it);
        }
    }

    [[nodiscard]] std::optional<Price> best_price() const {
        if (levels.empty()) return std::nullopt;
        return levels.begin()->first;
    }

    [[nodiscard]] PriceLevel* best_level() {
        if (levels.empty()) return nullptr;
        return &levels.begin()->second;
    }

    [[nodiscard]] bool empty() const { return levels.empty(); }

    // Market depth snapshot (up to N levels)
    [[nodiscard]] std::vector<std::pair<Price, Quantity>>
    depth(size_t n = 5) const {
        std::vector<std::pair<Price, Quantity>> out;
        out.reserve(n);
        for (auto& [p, lvl] : levels) {
            if (out.size() >= n) break;
            out.emplace_back(p, lvl.total_qty);
        }
        return out;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
//  OrderBook
// ─────────────────────────────────────────────────────────────────────────────
class OrderBook {
public:
    explicit OrderBook(std::string sym) : symbol_{std::move(sym)} {}

    // ── Submit an order ────────────────────────────────────────────────────
    std::vector<Trade> submit(Side side, OrderType type,
                              Price price, Quantity qty) {
        OrderId id = next_id_.fetch_add(1, std::memory_order_relaxed);
        auto* o    = new Order{id, side, type, price, qty, symbol_};
        orders_[id] = o;

        std::vector<Trade> trades = match(o);

        // Resting logic
        if (o->is_active()) {
            if (type == OrderType::IOC) {
                // Cancel whatever's left
                o->status = OrderStatus::CANCELLED;
            } else if (type == OrderType::LIMIT) {
                add_to_book(o);
            } else {
                // MARKET order with unfilled quantity — cancel remainder
                o->status = OrderStatus::CANCELLED;
            }
        }

        return trades;
    }

    // ── Cancel a resting order ─────────────────────────────────────────────
    bool cancel(OrderId id) {
        auto it = orders_.find(id);
        if (it == orders_.end()) return false;
        Order* o = it->second;
        if (!o->is_active()) return false;

        remove_from_book(o);
        o->status = OrderStatus::CANCELLED;
        return true;
    }

    // ── Accessors ──────────────────────────────────────────────────────────
    [[nodiscard]] std::optional<Price> best_bid() const {
        return bids_.best_price();
    }
    [[nodiscard]] std::optional<Price> best_ask() const {
        return asks_.best_price();
    }
    [[nodiscard]] std::optional<Price> mid_price() const {
        auto b = best_bid(), a = best_ask();
        if (!b || !a) return std::nullopt;
        return (*b + *a) / 2;
    }
    [[nodiscard]] std::optional<Price> spread() const {
        auto b = best_bid(), a = best_ask();
        if (!b || !a) return std::nullopt;
        return *a - *b;
    }

    [[nodiscard]] const std::string& symbol() const { return symbol_; }

    [[nodiscard]] uint64_t total_trades()  const { return trade_counter_; }
    [[nodiscard]] uint64_t total_volume()  const { return total_volume_;  }
    [[nodiscard]] uint64_t order_count()   const { return next_id_.load(); }

    // Market depth (5 levels each side)
    void print_depth(std::ostream& os = std::cout) const {
        auto asks = asks_.depth(5);
        auto bids = bids_.depth(5);

        os << "\n╔══════════════════════════════════════════════╗\n";
        os << std::format("║  Order Book: {:^32s} ║\n", symbol_);
        os << "╠══════════════════════════════════════════════╣\n";
        os << "║        ASK (sell side)  — lowest first       ║\n";
        os << "╠═══════════════╦══════════════╦═══════════════╣\n";
        os << "║   Price ($)   ║  Quantity    ║  Value ($)    ║\n";
        os << "╠═══════════════╬══════════════╬═══════════════╣\n";

        // Print asks in reverse (so highest ask is at top visually)
        for (auto it = asks.rbegin(); it != asks.rend(); ++it) {
            auto [p, q] = *it;
            double price_d = p / 100.0;
            double val     = price_d * q;
            os << std::format("║  {:>10.2f}   ║  {:>9}   ║  {:>10.2f}   ║\n",
                               price_d, q, val);
        }

        // Spread row
        if (auto sp = spread()) {
            os << "╠═══════════════╩══════════════╩═══════════════╣\n";
            os << std::format("║    Spread: {:>6.2f}  │  Mid: {:>9.2f}        ║\n",
                               *sp / 100.0,
                               mid_price().value_or(0) / 100.0);
            os << "╠═══════════════╦══════════════╦═══════════════╣\n";
        } else {
            os << "╠═══════════════╦══════════════╦═══════════════╣\n";
        }

        for (auto& [p, q] : bids) {
            double price_d = p / 100.0;
            double val     = price_d * q;
            os << std::format("║  {:>10.2f}   ║  {:>9}   ║  {:>10.2f}   ║\n",
                               price_d, q, val);
        }

        os << "╠═══════════════╩══════════════╩═══════════════╣\n";
        os << "║        BID (buy side)   — highest first      ║\n";
        os << "╚══════════════════════════════════════════════╝\n";

        os << std::format("\n  Trades: {:>8}  │  Volume: {:>10}  │  Orders: {:>8}\n",
                           trade_counter_, total_volume_, next_id_.load());
        os << "\n";
    }

    // Last N trades
    void print_trades(std::ostream& os = std::cout, size_t n = 10) const {
        os << "\n╔══════════════════════════════════════════════════════════╗\n";
        os << "║                    Recent Trades                        ║\n";
        os << "╠═══════╦══════════╦══════════╦══════════╦════════════════╣\n";
        os << "║  #    ║ Agg.Side ║  Price   ║   Qty    ║  Trade ID      ║\n";
        os << "╠═══════╬══════════╬══════════╬══════════╬════════════════╣\n";

        size_t start = trade_history_.size() > n
                           ? trade_history_.size() - n : 0;
        for (size_t i = start; i < trade_history_.size(); ++i) {
            const auto& t = trade_history_[i];
            std::string side_s = (t.aggressor_side == Side::BUY) ? "BUY " : "SELL";
            os << std::format("║ {:>4}  ║   {:>4}   ║ {:>8.2f} ║ {:>8}  ║ {:>14} ║\n",
                               i + 1, side_s,
                               t.exec_price / 100.0,
                               t.exec_qty,
                               t.trade_id);
        }
        os << "╚═══════╩══════════╩══════════╩══════════╩════════════════╝\n\n";
    }

private:
    std::string symbol_;

    // Bids: highest price first
    HalfBook<std::greater<Price>> bids_;
    // Asks: lowest price first
    HalfBook<std::less<Price>>    asks_;

    // order id → Order* (owns memory)
    std::unordered_map<OrderId, Order*> orders_;

    std::atomic<OrderId> next_id_{1};
    std::atomic<uint64_t> trade_id_gen_{1};

    std::vector<Trade> trade_history_;
    uint64_t trade_counter_{0};
    uint64_t total_volume_{0};

    // ── Matching engine ────────────────────────────────────────────────────
    std::vector<Trade> match(Order* aggressor) {
        std::vector<Trade> trades;

        HalfBook<std::greater<Price>>* passive_book_bid = nullptr;
        HalfBook<std::less<Price>>*    passive_book_ask = nullptr;

        // Aggressor BUY → match against ASK book
        // Aggressor SELL → match against BID book
        bool buy_aggressor = (aggressor->side == Side::BUY);

        while (aggressor->leaves_qty > 0) {
            Order* passive = nullptr;

            if (buy_aggressor) {
                PriceLevel* lvl = asks_.best_level();
                if (!lvl) break;
                // Market order always crosses; limit order must cross spread
                if (aggressor->type == OrderType::LIMIT &&
                    aggressor->price < lvl->price)
                    break;
                passive = lvl->orders.front();
            } else {
                PriceLevel* lvl = bids_.best_level();
                if (!lvl) break;
                if (aggressor->type == OrderType::LIMIT &&
                    aggressor->price > lvl->price)
                    break;
                passive = lvl->orders.front();
            }

            if (!passive) break;

            // Fill quantity = min of both leaves
            Quantity fill = std::min(aggressor->leaves_qty, passive->leaves_qty);
            Price    exec = passive->price;   // passive sets the price

            // Update quantities
            aggressor->leaves_qty -= fill;
            passive->leaves_qty   -= fill;

            // Update statuses
            aggressor->status = (aggressor->leaves_qty == 0)
                                     ? OrderStatus::FILLED
                                     : OrderStatus::PARTIAL;
            passive->status   = (passive->leaves_qty == 0)
                                     ? OrderStatus::FILLED
                                     : OrderStatus::PARTIAL;

            // Build trade record
            Trade t{
                trade_id_gen_.fetch_add(1, std::memory_order_relaxed),
                aggressor->id,
                passive->id,
                exec,
                fill,
                aggressor->side,
                now_ns()
            };
            trades.push_back(t);
            trade_history_.push_back(t);
            ++trade_counter_;
            total_volume_ += fill;

            // If passive is fully filled, remove from book
            if (passive->leaves_qty == 0) {
                remove_from_book(passive);
            } else {
                // Partial fill — update level's total_qty
                if (buy_aggressor) {
                    asks_.levels.at(passive->price).total_qty -= fill;
                } else {
                    bids_.levels.at(passive->price).total_qty -= fill;
                }
            }
        }

        return trades;
    }

    void add_to_book(Order* o) {
        if (o->side == Side::BUY)  bids_.add_order(o);
        else                        asks_.add_order(o);
    }

    void remove_from_book(Order* o) {
        if (o->side == Side::BUY)  bids_.remove_order(o);
        else                        asks_.remove_order(o);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
//  Throughput Benchmark
// ─────────────────────────────────────────────────────────────────────────────
struct BenchResult {
    uint64_t   orders;
    uint64_t   trades;
    double     elapsed_ms;
    double     orders_per_sec;
    double     trades_per_sec;
    double     avg_latency_ns;
};

BenchResult run_benchmark(uint64_t n_orders = 1'000'000) {
    OrderBook book{"BENCH"};

    // Seed a realistic book with 20 levels each side around $100
    for (int i = 0; i < 20; ++i) {
        Price ask = 10000 + (i + 1) * TICK;
        Price bid = 10000 - (i + 1) * TICK;
        book.submit(Side::SELL, OrderType::LIMIT, ask, 100 + i * 10);
        book.submit(Side::BUY,  OrderType::LIMIT, bid, 100 + i * 10);
    }

    std::mt19937_64 rng{42};
    std::uniform_int_distribution<int>  side_dist(0, 1);
    std::uniform_int_distribution<int>  type_dist(0, 2);
    std::uniform_int_distribution<int>  qty_dist(1, 200);
    std::normal_distribution<double>    price_dist(10000.0, 5.0);

    std::vector<int64_t> latencies;
    latencies.reserve(n_orders);

    auto t0 = Clock::now();
    uint64_t total_trades = 0;

    for (uint64_t i = 0; i < n_orders; ++i) {
        Side      side  = side_dist(rng) ? Side::BUY : Side::SELL;
        OrderType otype = [&]() {
            switch (type_dist(rng)) {
                case 0: return OrderType::LIMIT;
                case 1: return OrderType::MARKET;
                default: return OrderType::IOC;
            }
        }();
        Price    price = static_cast<Price>(price_dist(rng));
        Quantity qty   = static_cast<Quantity>(qty_dist(rng));

        auto t_start = now_ns();
        auto trades  = book.submit(side, otype, price, qty);
        auto t_end   = now_ns();

        latencies.push_back(t_end - t_start);
        total_trades += trades.size();
    }

    auto t1      = Clock::now();
    double ms    = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double ops_s = n_orders / (ms / 1000.0);
    double trd_s = total_trades / (ms / 1000.0);
    double avg_l = std::accumulate(latencies.begin(), latencies.end(), 0LL) /
                   static_cast<double>(latencies.size());

    return {n_orders, total_trades, ms, ops_s, trd_s, avg_l};
}

// ─────────────────────────────────────────────────────────────────────────────
//  Demo: simulate a realistic session
// ─────────────────────────────────────────────────────────────────────────────
void run_demo() {
    std::cout << "\n";
    std::cout << "  ╔══════════════════════════════════════════════════════╗\n";
    std::cout << "  ║      HFT Limit Order Book Engine  ·  C++20          ║\n";
    std::cout << "  ╚══════════════════════════════════════════════════════╝\n\n";

    OrderBook book{"AAPL"};

    // ── Seed resting orders (market maker quotes) ────────────────────────
    std::cout << "  [1] Seeding market maker quotes...\n";
    struct Quote { Side s; Price p; Quantity q; };
    std::vector<Quote> seed = {
        {Side::SELL, 18510, 300}, {Side::SELL, 18505, 500},
        {Side::SELL, 18502, 200}, {Side::SELL, 18501, 800},
        {Side::SELL, 18500, 1000},
        {Side::BUY,  18499, 1000},{Side::BUY,  18498, 700},
        {Side::BUY,  18495, 400}, {Side::BUY,  18490, 600},
        {Side::BUY,  18485, 300},
    };
    for (auto& q : seed)
        book.submit(q.s, OrderType::LIMIT, q.p, q.q);

    book.print_depth();

    // ── Aggressive buy sweep ─────────────────────────────────────────────
    std::cout << "  [2] Aggressive BUY market order — 2500 shares...\n";
    auto trades = book.submit(Side::BUY, OrderType::MARKET, 0, 2500);
    std::cout << "      → " << trades.size() << " trade(s) generated\n";
    book.print_depth();
    book.print_trades();

    // ── Limit order that sits on the book ───────────────────────────────
    std::cout << "  [3] Limit BUY $184.90 × 5000 (rests on book)...\n";
    book.submit(Side::BUY, OrderType::LIMIT, 18490, 5000);
    book.print_depth();

    // ── IOC sell that partially fills ───────────────────────────────────
    std::cout << "  [4] IOC SELL $184.98 × 2000 (immediate or cancel)...\n";
    trades = book.submit(Side::SELL, OrderType::IOC, 18498, 2000);
    std::cout << "      → " << trades.size() << " trade(s), remainder cancelled\n";
    book.print_depth();
    book.print_trades();
}

// ─────────────────────────────────────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────────────────────────────────────
int main() {
    run_demo();

    // ── Benchmark ─────────────────────────────────────────────────────────
    std::cout << "\n  [5] Throughput benchmark — 1,000,000 orders...\n\n";
    auto res = run_benchmark(1'000'000);

    std::cout << "  ╔════════════════════════════════════════════╗\n";
    std::cout << "  ║           Benchmark Results                ║\n";
    std::cout << "  ╠════════════════════════════════════════════╣\n";
    std::cout << std::format("  ║  Orders submitted : {:>12}          ║\n", res.orders);
    std::cout << std::format("  ║  Trades generated : {:>12}          ║\n", res.trades);
    std::cout << std::format("  ║  Elapsed time     : {:>12.2f} ms     ║\n", res.elapsed_ms);
    std::cout << std::format("  ║  Orders / second  : {:>12.0f}          ║\n", res.orders_per_sec);
    std::cout << std::format("  ║  Trades / second  : {:>12.0f}          ║\n", res.trades_per_sec);
    std::cout << std::format("  ║  Avg latency      : {:>12.1f} ns      ║\n", res.avg_latency_ns);
    std::cout << "  ╚════════════════════════════════════════════╝\n\n";

    return 0;
}
