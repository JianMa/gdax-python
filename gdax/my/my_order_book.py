#
# gdax/my_order_book.py
# Jian Ma
#
# Live order book updated from the msg

from bintrees import RBTree
from decimal import Decimal


class OrderBook(object):
    def __init__(self, product_id='BTC-USD', feed=None, log_to=None):
        self._product_id = product_id
        self._asks = RBTree()
        self._bids = RBTree()
        self._sequence = -1
        self._current_ticker = None
        self._feed = feed

    @property
    def product_id(self):
        """ Currently OrderBook only supports a single product even though it is stored as a list of products. """
        return self.product_id

    def reset_book(self, snapshot):
        self._asks = RBTree()
        self._bids = RBTree()
        for bid in snapshot['bids']:
            self._add({
                'id': bid[2],
                'side': 'buy',
                'price': Decimal(bid[0]),
                'size': Decimal(bid[1])
            })
        for ask in snapshot['asks']:
            self._add({
                'id': ask[2],
                'side': 'sell',
                'price': Decimal(ask[0]),
                'size': Decimal(ask[1])
            })
        self._sequence = snapshot['sequence']

    def on_message(self, message):
        sequence = message['sequence']
        if self._sequence == -1:
            print("Expected snapshot before any message")
            sys.exit()
            # self.reset_book()
            return
        if sequence <= self._sequence:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            return
        elif sequence > self._sequence + 1:
            self.on_sequence_gap(self._sequence, sequence)
            # return

        msg_type = message['type']
        if msg_type == 'open':
            self._add(message)
        elif msg_type == 'done' and 'price' in message:
            self._remove(message)
        elif msg_type == 'match':
            self._match(message)
            self._current_ticker = message
        elif msg_type == 'change':
            self._change(message)

        self._sequence = sequence

    def on_sequence_gap(self, gap_start, gap_end):
        # self.reset_book()
        print('Error: messages missing ({} - {}). ignoring the gap.'.format(
            gap_start, gap_end, self._sequence))

    def get_current_ticker(self):
        return self._current_ticker

    def get_current_book(self):
        result = {
            'sequence': self._sequence,
            'asks': [],
            'bids': [],
        }
        for ask in self._asks:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_ask = self._asks[ask]
            except KeyError:
                continue
            for order in this_ask:
                result['asks'].append([order['price'], order['size'], order['id']])
        for bid in self._bids:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_bid = self._bids[bid]
            except KeyError:
                continue

            for order in this_bid:
                result['bids'].append([order['price'], order['size'], order['id']])
        return result

    def get_ask(self):
        return self._asks.min_key()

    def get_asks(self, price):
        return self._asks.get(price)

    def remove_asks(self, price):
        self._asks.remove(price)

    def set_asks(self, price, asks):
        self._asks.insert(price, asks)

    def get_bid(self):
        return self._bids.max_key()

    def get_bids(self, price):
        return self._bids.get(price)

    def remove_bids(self, price):
        self._bids.remove(price)

    def set_bids(self, price, bids):
        self._bids.insert(price, bids)

    @staticmethod
    def meet_min_diff_price(low_price, high_price, min_diff_price):
        if low_price is None or high_price is None or min_diff_price is None:
            return False
        return high_price - low_price > min_diff_price

    @staticmethod
    def meet_max_size(total_size, max_size):
        if total_size is None or max_size is None:
            return False
        return total_size > max_size

    def get_aggr_bids(self, min_diff_price=None, max_size=None):
        max_bid = self.get_bid()
        total_size = 0
        aggr_bids = list()
        for price, bid_orders in reversed(list(self._bids.items())):
            if (OrderBook.meet_min_diff_price(price, max_bid, min_diff_price) and
                    OrderBook.meet_max_size(total_size, max_size)):
                break
            size = sum(bid_order['size'] for bid_order in bid_orders)
            num_orders = len(bid_orders)
            aggr_bids.append({'price': price, 'size': size, 'num_orders': num_orders})
            total_size += size
        return aggr_bids

    def get_aggr_asks(self, min_diff_price=None, max_size=None):
        min_ask = self.get_ask()
        total_size = 0
        aggr_asks = list()
        for price, ask_orders in self._asks.items():
            if (OrderBook.meet_min_diff_price(min_ask, price, min_diff_price) and
                    OrderBook.meet_max_size(total_size, max_size)):
                break
            size = sum(ask_order['size'] for ask_order in ask_orders)
            num_orders = len(ask_orders)
            aggr_asks.append({'price': price, 'size': size, 'num_orders': num_orders})
            total_size += size
        return aggr_asks

    # Internal operations
    def _add(self, order):
        order = {
            'id': order.get('order_id') or order['id'],
            'side': order['side'],
            'price': Decimal(order['price']),
            'size': Decimal(order.get('size') or order['remaining_size'])
        }
        if order['side'] == 'buy':
            bids = self.get_bids(order['price'])
            if bids is None:
                bids = [order]
            else:
                bids.append(order)
            self.set_bids(order['price'], bids)
        else:
            asks = self.get_asks(order['price'])
            if asks is None:
                asks = [order]
            else:
                asks.append(order)
            self.set_asks(order['price'], asks)

    def _remove(self, order):
        price = Decimal(order['price'])
        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if bids is not None:
                bids = [o for o in bids if o['id'] != order['order_id']]
                if len(bids) > 0:
                    self.set_bids(price, bids)
                else:
                    self.remove_bids(price)
        else:
            asks = self.get_asks(price)
            if asks is not None:
                asks = [o for o in asks if o['id'] != order['order_id']]
                if len(asks) > 0:
                    self.set_asks(price, asks)
                else:
                    self.remove_asks(price)

    def _match(self, order):
        if self._feed is not None:
            self._feed._match(order)

        size = Decimal(order['size'])
        price = Decimal(order['price'])

        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if not bids:
                return
            assert bids[0]['id'] == order['maker_order_id']
            if bids[0]['size'] == size:
                self.set_bids(price, bids[1:])
            else:
                bids[0]['size'] -= size
                self.set_bids(price, bids)
        else:
            asks = self.get_asks(price)
            if not asks:
                return
            assert asks[0]['id'] == order['maker_order_id']
            if asks[0]['size'] == size:
                self.set_asks(price, asks[1:])
            else:
                asks[0]['size'] -= size
                self.set_asks(price, asks)

    def _change(self, order):
        try:
            new_size = Decimal(order['new_size'])
        except KeyError:
            return

        try:
            price = Decimal(order['price'])
        except KeyError:
            return

        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if bids is None or not any(o['id'] == order['order_id'] for o in bids):
                return
            index = [b['id'] for b in bids].index(order['order_id'])
            bids[index]['size'] = new_size
            self.set_bids(price, bids)
        else:
            asks = self.get_asks(price)
            if asks is None or not any(o['id'] == order['order_id'] for o in asks):
                return
            index = [a['id'] for a in asks].index(order['order_id'])
            asks[index]['size'] = new_size
            self.set_asks(price, asks)

        tree = self._asks if order['side'] == 'sell' else self._bids
        node = tree.get(price)

        if node is None or not any(o['id'] == order['order_id'] for o in node):
            return


if __name__ == '__main__':
    import sys
    import time
    import datetime as dt


    class OrderBookConsole(OrderBook):
        ''' Logs real-time changes to the bid-ask spread to the console '''

        def __init__(self, product_id=None):
            super(OrderBookConsole, self).__init__(product_id=product_id)

            # latest values of bid-ask spread
            self._bid = None
            self._ask = None
            self._bid_depth = None
            self._ask_depth = None

        def on_message(self, message):
            super(OrderBookConsole, self).on_message(message)

            # Calculate newest bid-ask spread
            bid = self.get_bid()
            bids = self.get_bids(bid)
            bid_depth = sum([b['size'] for b in bids])
            ask = self.get_ask()
            asks = self.get_asks(ask)
            ask_depth = sum([a['size'] for a in asks])

            if self._bid == bid and self._ask == ask and self._bid_depth == bid_depth and self._ask_depth == ask_depth:
                # If there are no changes to the bid-ask spread since the last update, no need to print
                pass
            else:
                # If there are differences, update the cache
                self._bid = bid
                self._ask = ask
                self._bid_depth = bid_depth
                self._ask_depth = ask_depth
                print('{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
                    dt.datetime.now(), self.product_id, bid_depth, bid, ask_depth, ask))

    order_book = OrderBookConsole()
    order_book.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        order_book.close()

    if order_book.error:
        sys.exit(1)
    else:
        sys.exit(0)
