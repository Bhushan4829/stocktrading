import threading
import random
import time

# Constants
MAX_TICKERS = 1024  # Total number of supported stocks

class TradeOrder:
    """ Represents a Buy/Sell order in the stock trading system """
    def __init__(self, order_type, ticker_symbol, quantity, price):
        self.order_type = order_type  # "Buy" or "Sell"
        self.ticker_symbol = ticker_symbol
        self.quantity = quantity
        self.price = price
        self.next_order = None  # Pointer for linked list structure

class OrderBook:
    """ Manages Buy and Sell orders for a single stock ticker """
    def __init__(self):
        self.buy_orders = None  # Descending sorted linked list (highest price first)
        self.sell_orders = None  # Ascending sorted linked list (lowest price first)
        self.lock = threading.Lock()  # Ensures safe concurrent access

    def add_order(self, order_type, ticker_symbol, quantity, price):
        """ Adds an order to the appropriate linked list while maintaining sorting """
        new_order = TradeOrder(order_type, ticker_symbol, quantity, price)
        
        with self.lock:  # Ensuring thread safety
            if order_type == "Buy":
                self.buy_orders = self._insert_sorted(self.buy_orders, new_order, descending=True)
            else:
                self.sell_orders = self._insert_sorted(self.sell_orders, new_order, descending=False)

        # Try matching orders immediately after insertion
        self.match_orders()

    def _insert_sorted(self, head, new_order, descending):
        """ Inserts an order into a sorted linked list in O(n) time """
        if not head or (descending and new_order.price > head.price) or (not descending and new_order.price < head.price):
            new_order.next_order = head
            return new_order
        
        previous, current = None, head
        while current and ((descending and new_order.price <= current.price) or (not descending and new_order.price >= current.price)):
            previous, current = current, current.next_order
        
        previous.next_order, new_order.next_order = new_order, current
        return head

    def match_orders(self):
        """ Matches Buy and Sell orders based on price in O(n) complexity """
        with self.lock:
            while self.buy_orders and self.sell_orders and self.buy_orders.price >= self.sell_orders.price:
                buy_order = self.buy_orders
                sell_order = self.sell_orders

                # Determine how many shares can be executed
                matched_quantity = min(buy_order.quantity, sell_order.quantity)

                print(f"TRADE EXECUTED: {matched_quantity} shares of {buy_order.ticker_symbol} at ${sell_order.price}")

                # Adjust remaining order quantities
                buy_order.quantity -= matched_quantity
                sell_order.quantity -= matched_quantity

                # Remove orders that have been fully executed
                if buy_order.quantity == 0:
                    self.buy_orders = self.buy_orders.next_order
                if sell_order.quantity == 0:
                    self.sell_orders = self.sell_orders.next_order

class StockExchange:
    """ Manages order books for multiple stock tickers """
    def __init__(self):
        self.order_books = [OrderBook() for _ in range(MAX_TICKERS)]

    def add_order(self, order_type, ticker_symbol, quantity, price):
        """ Adds an order to the corresponding stock's order book """
        ticker_index = hash(ticker_symbol) % MAX_TICKERS  # Hash function for ticker lookup
        self.order_books[ticker_index].add_order(order_type, ticker_symbol, quantity, price)

# Initialize Stock Exchange
stock_exchange = StockExchange()

# Simulated Trading System
def simulate_market_activity():
    stock_symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]
    
    for _ in range(50):  # Simulate 50 transactions
        order_type = random.choice(["Buy", "Sell"])
        ticker_symbol = random.choice(stock_symbols)
        quantity = random.randint(1, 100)
        price = round(random.uniform(100, 500), 2)

        stock_exchange.add_order(order_type, ticker_symbol, quantity, price)
        time.sleep(random.uniform(0.1, 0.5))  # Simulate real-time trading activity

trading_threads = []
for _ in range(5):  # Five brokers placing trades concurrently
    thread = threading.Thread(target=simulate_market_activity)
    thread.start()
    trading_threads.append(thread)

# Wait for all threads to finish execution
for thread in trading_threads:
    thread.join()
