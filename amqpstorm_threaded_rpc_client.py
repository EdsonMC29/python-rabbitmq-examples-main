"""
This is a simple example on how to use Flask and Asynchronous RPC calls.

I kept this simple, but if you want to use this properly you will need
to expand the concept.

Things that are not included in this example.
    - Reconnection strategy. ✅ (Now included)

    - Consider implementing utility functionality for checking and getting
      responses.

        def has_response(correlation_id)
        def get_response(correlation_id)

Apache/wsgi configuration.
    - Each process you start with apache will create a new connection to
      RabbitMQ.

    - I would recommend depending on the size of the payload that you have
      about 100 threads per process. If the payload is larger, it might be
      worth to keep a lower thread count per process.

For questions feel free to email me: me@eandersson.net
"""
__author__ = 'eandersson'

import os
import threading
from time import sleep
from flask import Flask

import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

class RpcClient(object):
    """Asynchronous Rpc client with reconnection support."""

    def __init__(self, host, username, password, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self._connect_with_retry()

    def _connect_with_retry(self):
        """Try to connect to RabbitMQ with retry logic."""
        while True:
            try:
                self.open()
                print("[RPC CLIENT] Connected to RabbitMQ")
                break
            except amqpstorm.exception.AMQPConnectionError as e:
                print(f"[RPC CLIENT] Connection failed: {e}. Retrying in 5 seconds...")
                sleep(5)

    def open(self):
        """Open Connection."""
        self.connection = amqpstorm.Connection(
            self.host,
            self.username,
            self.password,
            virtual_host='xguypqlh',  # CloudAMQP virtual host
            heartbeat=60  # Heartbeat every 60 seconds
        )
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, no_ack=True,
                                   queue=self.callback_queue)
        self._create_process_thread()
        self._start_keep_alive()

    def _create_process_thread(self):
        """Create a thread responsible for consuming messages."""
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        """Start consuming RPC response messages."""
        try:
            self.channel.start_consuming(to_tuple=False)
        except amqpstorm.exception.AMQPConnectionError:
            print("[RPC CLIENT] Lost connection while consuming.")
            self._connect_with_retry()

    def _on_response(self, message):
        """Handle incoming RPC response."""
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        """Send an RPC request and return the correlation ID."""
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        return message.correlation_id

    def _start_keep_alive(self):
        """Start thread to keep connection alive and reconnect if needed."""
        thread = threading.Thread(target=self._keep_alive)
        thread.daemon = True
        thread.start()

    def _keep_alive(self):
        """Periodically check connection and reconnect if lost."""
        while True:
            try:
                self.connection.process_data_events()
            except amqpstorm.exception.AMQPConnectionError:
                print("[RPC CLIENT] Connection lost during keep-alive. Reconnecting...")
                self._connect_with_retry()
            sleep(30)


@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Simple Flask route to make asynchronous RPC calls."""
    corr_id = RPC_CLIENT.send_request(payload)

    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)

    return RPC_CLIENT.queue[corr_id]


if __name__ == '__main__':
    RPC_CLIENT = RpcClient(
        host='jaragua.lmq.cloudamqp.com',
        username='xguypqlh',
        password='FywGOTaF_vYoLWQEatP1WTakBoG1X4Qs',
        rpc_queue='rpc_queue'
    )

    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
