# gdax/scheduler.py
# original author: Jian
#
#
# Template object to receive messages from the gdax Websocket Feed

from __future__ import print_function
import datetime
import json
import base64
import hmac
import hashlib
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException
from gdax_auth import get_auth_headers
import queue

from my.my_order_book import OrderBook


trade_size_str = "0.2"


def dt_str(dt):
    return dt.strftime("%Y%m%d %H:%M:%S.%f")

class Trader(object):
    """Trader object must run in the Scheduler thread"""
    def __init__(self, product_id, order_book, api_key, api_secret, api_passphrase):
        from authenticated_client import AuthenticatedClient
        self._product_id = product_id
        self._order_book = order_book
        self._ac = AuthenticatedClient(api_key, api_secret, api_passphrase)

        # status depends on the strategy
        """
        ready
        buy_sent
        sell_sent
        """
        self._status = "ready"
        self._buy_order_id = None
        self._sell_order_id = None

    def on_mkt_msg_end(self):
        best_bid = self._order_book.get_bid()
        best_ask = self._order_book.get_ask()
        if best_ask - best_bid > 1.0:
            now = datetime.datetime.now()
            print("WARNING: mkt is wide, now=%s best_bid=%.2f best_ask=%.2f" % (dt_str(now), best_bid, best_ask))

    def on_user_msg(self, user_msg):
        if user_msg == 'b':
            pass
        elif user_msg == 'c':
            self.cancel()
        elif user_msg == 'k':
            best_bid = self._order_book.get_bid()
            best_ask = self._order_book.get_ask()
            print("INFO: best_bid=%.2f best_ask=%.2f" % (best_bid, best_ask))

    def buy(self, buy_price):
        buy_order_id = None
        try:
            buy_price_str = "%.2f" % buy_price
            buy_response = self._ac.buy(price=buy_price_str, size=trade_size_str,
                                        product_id=self._product_id, post_only=True)
            buy_order_id = buy_response['id']
            print("NOTICE: buy_order is sent: buy_order_id=%s buy_price=%s" % (buy_order_id, buy_price_str))
            self._status = 'buy_sent'
        except Exception as e:
            self._ac.cancel_all(product_id=self._product_id)
            print("ERROR: problem in buy, cancel all: e=%s buy_response=%s" % (e, buy_response))
            self._status = 'ready'
        finally:
            print("buy_response=%s" % buy_response)
        return buy_order_id

    def sell(self, sell_price):
        sell_order_id = None
        try:
            sell_price_str = '%.2f' % sell_price
            sell_response = self._ac.sell(price=sell_price_str, size=trade_size_str,
                                          product_id=self._product_id, post_only=True)
            sell_order_id = sell_response['id']
            print("NOTICE: sell_order is sent: sell_order_id=%s sell_price=%s" % (sell_order_id, sell_price_str))
            self._status = "sell_sent"
        except Exception as e:
            self._ac.cancel_all(product_id=self._product_id)
            print("ERROR: problem in sell, cancel all: e=%s sell_response=%s" % (e, sell_response))
            self._status = "ready"
        finally:
            print("sell_response=%s" % sell_response)
        return sell_order_id

    def cancel(self):
        try:
            cancel_response = self._ac.cancel_all(product_id=self._product_id)
            print("NOTICE: cancel is sent: cancel_response=%s" % (cancel_response))
        except Exception as e:
            print("ERROR: problem in cancel" % e)


class Scheduler(object):
    def __init__(self, url="wss://ws-feed.gdax.com", products=None, channels=None, message_type="subscribe",
                 should_print=True,
                 auth=False, api_key="", api_secret="", api_passphrase="",  out_filename=None):
        if products is None or len(products) != 1:
            print("ERROR: it only supports one product_id")
            sys.eixt()
        self.url = url
        self.products = products
        self.channels = channels
        self.type = message_type

        self.error = None
        self.running_code = None

        self.ws = None
        self.thread = None

        self.should_print = should_print

        self.auth = auth
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.user_msg_queue = queue.Queue()
        self.out_file = sys.stdout if out_filename is None else open(out_filename, 'w')

        self.order_book = OrderBook()
        self.trader = Trader(self.products[0], self.order_book, api_key, api_secret, api_passphrase)

    def start(self):
        def _go():
            while self.running_code != "stop":
                self._connect()
                self._listen()
                self._disconnect()

        self.running_code = None
        self.on_open()
        self.thread = Thread(target=_go)
        self.thread.start()

    def _connect(self):
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        if self.channels is None:
            sub_params = {'type': 'subscribe', 'product_ids': self.products}
        else:
            sub_params = {'type': 'subscribe', 'product_ids': self.products, 'channels': self.channels}

        if self.auth:
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self'
            sub_params.update(get_auth_headers(timestamp, message, self.api_key,  self.api_secret, self.api_passphrase))

        self.ws = create_connection(self.url)
        self.ws.send(json.dumps(sub_params))

        if self.type == "heartbeat":
            sub_params = {"type": "heartbeat", "on": True}
        else:
            sub_params = {"type": "heartbeat", "on": False}
        self.ws.send(json.dumps(sub_params))

    def _listen(self):
        from public_client import PublicClient
        snapshot = PublicClient().get_product_order_book(product_id=self.products[0], level=3)
        print(snapshot, file=self.out_file)
        self.order_book.reset_book(snapshot)

        # Avoid string comparison
        last_hb_time = None
        while self.running_code is None:
            try:
                epoch_now_sec = time.time()
                int_epoch_now_sec = int(epoch_now_sec)
                if (last_hb_time != int_epoch_now_sec) and (int_epoch_now_sec % 30 == 0):
                    # Set a 30 second ping to keep connection alive
                    self.ws.ping("keepalive")
                    print("Send keepalive HB: last_hb_time=%s epoch_now_sec=%s" % (last_hb_time, epoch_now_sec), flush=True)
                    last_hb_time = int_epoch_now_sec

                for i in range(10):
                    data = self.ws.recv()
                    # print('data(%s)=%s' % (type(data), data))
                    mkt_msg = json.loads(data)
                    # TODO: will put raw_msg in
                    self.order_book.on_message(mkt_msg)
                self.trader.on_mkt_msg_end()
                print(data, file=self.out_file)

                if not self.user_msg_queue.empty():
                    user_msg = self.user_msg_queue.get(block=True)
                    if user_msg in {"stop", "exit", "close"}:
                        print("User stops it", flush=True)
                        self.running_code = "stop"
                    else:
                        self.trader.on_user_msg(user_msg)
            except WebSocketConnectionClosedException as e:
                self.on_error(e, data)
            except ValueError as e:
                self.on_error(e, data)
            except Exception as e:
                self.on_error(e, data)
            else:
                # self.on_message(msg)
                pass

    def _disconnect(self):
        if self.type == "heartbeat":
            self.ws.send(json.dumps({"type": "heartbeat", "on": False}))
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()

    def on_open(self):
        if self.should_print:
            print("-- Subscribed! --\n")

    def on_close(self):
        if self.should_print:
            print("\n-- Socket Closed --")

    def on_message(self, msg):
        if self.should_print:
            print(msg)

    def on_error(self, e, data=None):
        self.running_code = "reconnect"
        print('{} - data: {}'.format(e, data))

    # Public API for main thread
    def send_user_msg_to_scheduler(self, user_msg):
        """It's a thread-safe way for main thread to send user_msg"""
        self.user_msg_queue.put(user_msg, block=True)

    def close(self):
        self.send_user_msg_to_scheduler("stop")
        self.thread.join()

if __name__ == "__main__":
    import sys
    # import gdax
    import time


    # class MyWebsocketClient(gdax.Scheduler):
    #     def on_open(self):
    #         self.url = "wss://ws-feed.gdax.com/"
    #         self.products = ["BTC-USD", "ETH-USD"]
    #         self.message_count = 0
    #         print("Let's count the messages!")
    #
    #     def on_message(self, msg):
    #         print(json.dumps(msg, indent=4, sort_keys=True))
    #         self.message_count += 1
    #
    #     def on_close(self):
    #         print("-- Goodbye! --")

    api_key = "02f6144c56888a28e55cfb0005f7dab6"
    api_secret = "W1DDu/KCM0IzsrStCKsH7WY5mY9tJ2jyx148udCek35XYl5Z7yyIiNvtZdZujssEW7KILeqIaA8qQaQ4CZO1Bg=="
    api_passphrase = "3ut0w1wvnpp"

    wsClient = Scheduler(
        products=['LTC-USD'],
        api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase, out_filename=sys.argv[1])
    wsClient.start()
    print(wsClient.url, wsClient.products)
    try:
        #while True:
            # print("\nMessageCount =", "%i \n" % wsClient.message_count)
        #    time.sleep(1)
        time.sleep(3600 * 24 + 300)
    except KeyboardInterrupt:
        wsClient.close()

    if wsClient.error:
        sys.exit(1)
    else:
        sys.exit(0)
