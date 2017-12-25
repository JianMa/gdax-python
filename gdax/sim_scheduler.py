# gdax/scheduler.py
# original author: Jian
#

import datetime
import time
import logging

from my.my_order_book import OrderBook


logger = logging.getLogger(__name__)


class SimScheduler(object):
    def __init__(self, products=None,
                 in_filename=None,
                 order_book=None, trader=None):
        if products is None or len(products) != 1:
            logger.error("it only supports one product_id")
            sys.eixt()
        self.products = products

        self._in_file = open(in_filename, 'r')

        self.order_book = order_book
        self.trader = trader

    def _connect(self):
        logger.critical("Connecting...")
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

    def _read_from_file(self):
        for l in self._in_file:
            event = eval(l)
            recv_time = event['recv_time']
            msg_type = event['msg_type']
            recv_msg = event['recv_msg']

            now = recv_time
            if msg_type == 'snapshot':
                self.order_book.reset_book(recv_msg)
            elif msg_type == 'update':
                self.order_book.on_message(recv_msg)
                if self.trader:
                    self.trader.on_mkt_msg_end(now)

    # Public API for main thread
    def send_user_msg_to_scheduler(self, user_msg):
        pass

    def start(self):
        pass

    def run(self):
        self._read_from_file()
        return 0

    def close(self):
        self.send_user_msg_to_scheduler("stop")


class SimTrader(object):
    def __init__(self, product_id, order_book):
        self._product_id = product_id
        self._order_book = order_book

        # status depends on the strategy
        """
        ready
        buy_sent
        sell_sent
        """
        self._status = "ready"
        self._buy_order_id = None
        self._sell_order_id = None
        self._cnt = 0

    def on_mkt_msg_end(self, now):
        best_bid = self._order_book.get_bid()
        best_ask = self._order_book.get_ask()
        if best_ask - best_bid > 1.0:
            logger.warning("mkt is wide, now=%s best_bid=%.2f best_ask=%.2f" % (str(now), best_bid, best_ask))
        if self._cnt % 10000 == 0:
            # logger.info("now=%s best_bid=%.2f best_ask=%.2f" % (str(now), best_bid, best_ask))
            print("now=%s best_bid=%.2f best_ask=%.2f" % (str(now), best_bid, best_ask))
        self._cnt += 1


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='SchedulerSim For Trading')
    parser.add_argument('-t', '--trading_type', dest='trading_type', default='TRADER',
                        help='Choices of TRADER')
    parser.add_argument('-i', '--in_file', dest='in_file',
                        help='Specify output file for TRADER')
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level='DEBUG',
    )

    trading_type = args.trading_type.upper()
    product_id = 'LTC-USD'
    order_book = OrderBook()
    trader = SimTrader(product_id, order_book)
    scheduler = SimScheduler(
        products=['LTC-USD'],
        in_filename=args.in_file,
        order_book=order_book, trader=trader)
    scheduler.start()
    error = scheduler.run()

    if error:
        sys.exit(1)
    else:
        sys.exit(0)
