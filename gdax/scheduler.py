# gdax/scheduler.py
# original author: Jian
#
#
# Template object to receive messages from the gdax Websocket Feed

from __future__ import print_function
import json
import base64
import hmac
import hashlib
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException
from gdax_auth import get_auth_headers
import queue

msg_queue = queue.Queue()


class Scheduler(object):
    def __init__(self, url="wss://ws-feed.gdax.com", products=None, message_type="subscribe", out_filename=None,
                 should_print=True, auth=False, api_key="", api_secret="", api_passphrase="", channels=None):
        self.url = url
        self.products = products
        self.channels = channels
        self.type = message_type
        self.stop = False
        self.error = None
        self.ws = None
        self.thread = None
        self.auth = auth
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.should_print = should_print
        self.out_file = sys.stdout if out_filename is None else open(out_filename, 'w')

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.stop = False
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

    def _on_user_msg(self, user_msg):
        print("ERROR: if see it")

    def _listen(self):
        while not self.stop:
            try:
                if int(time.time() % 30) == 0:
                    # Set a 30 second ping to keep connection alive
                    self.ws.ping("keepalive")
                data = self.ws.recv()
                # print('data(%s)=%s' % (type(data), data))
                try:
                    msg = json.loads(data)
                except Exception as e:
                    print("ERROR: exception e=%s data=%s" % (e, data))
                print(data, file=self.out_file)

                if not msg_queue.empty():
                    user_msg = msg_queue.get(block=True)
                    self._on_user_msg(user_msg)
            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
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

    def close(self):
        self.stop = True
        self.thread.join()

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
        self.error = e
        self.stop = True
        print('{} - data: {}'.format(e, data))


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

    wsClient = Scheduler(out_filename=sys.argv[1])
    wsClient.start()
    print(wsClient.url, wsClient.products)
    try:
        while True:
            # print("\nMessageCount =", "%i \n" % wsClient.message_count)
            time.sleep(1)
    except KeyboardInterrupt:
        wsClient.close()

    if wsClient.error:
        sys.exit(1)
    else:
        sys.exit(0)
