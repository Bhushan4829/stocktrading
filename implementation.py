import threading
import random
import time

# Constants
MAX_TICKERS = 1024  # Number of stocks supported

class Order:
    """ Order structure for stock buy/sell transactions """
    def __init__(self, order_type, ticker, quantity, price):
        self.order_type = order_type  # "Buy" or "Sell"
        self.ticker = ticker
        self.quantity = quantity
        self.price = price
        self.next = None  # For Linked List implementation

class OrderBook:
    """ Manages buy and sell orders for a single ticker symbol """
    def __init__(self):
        self.buy_head = None  # Descending sorted linked list (highest price first)
        self.sell_head = None  # Ascending sorted linked list (lowest price first)
        self.lock = threading.Lock()  # Ensures atomic operations

    def add_order(self, order_type, ticker, quantity, price):
        """ Adds an order to the appropriate list while keeping it sorted """
        new_order = Order(order_type, ticker, quantity, price)
        
        with self.lock:  # Ensuring atomic modifications
            if order_type == "Buy":
                self.buy_head = self._insert_sorted(self.buy_head, new_order, descending=True)
            else:
                self.sell_head = self._insert_sorted(self.sell_head, new_order, descending=False)
        
        # Attempt to match orders after adding
        self.match_orders()

    def _insert_sorted(self, head, new_order, descending):
        """ Inserts an order into a sorted linked list """
        if not head or (descending and new_order.price > head.price) or (not descending and new_order.price < head.price):
            new_order.next = head
            return new_order
        
        prev, curr = None, head
        while curr and ((descending and new_order.price <= curr.price) or (not descending and new_order.price >= curr.price)):
            prev, curr = curr, curr.next
        
        prev.next, new_order.next = new_order, curr
        return head

    def match_orders(self):
        """ Matches buy and sell orders based on price """
        with self.lock:
            while self.buy_head and self.sell_head and self.buy_head.price >= self.sell_head.price:
                buy_order = self.buy_head
                sell_order = self.sell_head

                # Determine transaction quantity
                trade_quantity = min(buy_order.quantity, sell_order.quantity)

                print(f"MATCH: {trade_quantity} shares of {buy_order.ticker} at ${sell_order.price}")

                # Update order quantities
                buy_order.quantity -= trade_quantity
                sell_order.quantity -= trade_quantity

                # Remove fully filled orders
                if buy_order.quantity == 0:
                    self.buy_head = self.buy_head.next
                if sell_order.quantity == 0:
                    self.sell_head = self.sell_head.next

class StockExchange:
    """ Manages order books for multiple tickers """
    def __init__(self):
        self.order_books = [OrderBook() for _ in range(MAX_TICKERS)]

    def add_order(self, order_type, ticker, quantity, price):
        """ Adds order to the appropriate ticker's order book """
        ticker_index = hash(ticker) % MAX_TICKERS  # Hash function to get ticker index
        self.order_books[ticker_index].add_order(order_type, ticker, quantity, price)

# Stock Exchange System
exchange = StockExchange()

# Simulating Random Order Execution
def simulate_orders():
    tickers = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]
    for _ in range(50):  # Simulate 50 transactions
        order_type = random.choice(["Buy", "Sell"])
        ticker = random.choice(tickers)
        quantity = random.randint(1, 100)
        price = round(random.uniform(100, 500), 2)

        exchange.add_order(order_type, ticker, quantity, price)
        time.sleep(random.uniform(0.1, 0.5))  # Simulate real-time transactions

# Running Simulated Trading in Threads
threads = []
for _ in range(5):  # 5 concurrent traders
    t = threading.Thread(target=simulate_orders)
    t.start()
    threads.append(t)

# Wait for threads to finish
for t in threads:
    t.join()
