import imp
import json
import logging
import os
import signal
import sys
from collections import namedtuple
from subprocess import PIPE, STDOUT

import pika
from honcho.process import Process
from honcho.procfile import Procfile


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


try:
    settings = imp.load_source('shove.settings', os.environ['SHOVE_SETTINGS_FILE'])
except (ImportError, KeyError):
    log.warning('Failed to import settings from environment variable, falling back to local file.')
    try:
        from . import settings
    except ImportError:
        log.warning('Error importing settings.py, did you copy settings.py-dist yet?')
        sys.exit(1)


Order = namedtuple('Order', ('project', 'command', 'log_key', 'log_queue'))


def parse_order(order_body):
    data = json.loads(order_body)
    try:
        return Order(project=data['project'], command=data['command'], log_key=data['log_key'],
                     log_queue=data['log_queue'])
    except KeyError:
        log.error('Could not parse order: `{0}`'.format(order_body))
        return None


def execute(order):
    """
    Execute the command for the project contained within the order.

    Commands are listed in a procfile within the project's repository. An order contains a project
    and a command to execute; if there are any issues with finding the procfile or the requested
    command within it, we log the issue and throw away the order.

    Commands are executed in a subprocess, but this blocks until the command is finished.

    :param order:
        The order to execute, in the form of an Order namedtuple.

    :returns:
        A tuple of (return_code, output). The output includes stdout and stderr together. If the
        command failed to execute, return_code will be 1 and the output will be an error message
        explaining the failure.
    """
    # Locate the procfile that lists the commands available for the requested project.
    project_path = settings.PROJECTS.get(order.project, None)
    if not project_path:
        msg = 'No project `{0}` found.'.format(order.project)
        log.warning(msg)
        return 1, msg

    procfile_path = os.path.join(project_path, 'bin', 'commands.procfile')
    try:
        with open(procfile_path, 'r') as f:
            procfile = Procfile(f.read())
    except IOError as err:
        msg = 'Error loading procfile for project `{0}`: {1}'.format(order.project, err)
        log.error(msg)
        return 1, msg

    command = procfile.commands.get(order.command)
    if not command:
        msg = 'No command `{0}` found in {1}'.format(order.command, procfile_path)
        log.warning(msg)
        return 1, msg

    # Execute the order and log the result. Sends stderr to stdout so we get everything in one
    # blob.
    p = Process(command, cwd=project_path, stdout=PIPE, stderr=STDOUT)
    output, err = p.communicate()
    log.info('Finished running {0} - returned {1}'.format(order.command, p.returncode))
    return p.returncode, output


def consume_message(adapter, message_body):
    """Consume a message from the queue."""
    order = parse_order(message_body)
    if order:
        log.info('Executing order: {0}'.format(order))
        return_code, output = execute(order)

        # Send the output of the command to the logging queue.
        body = json.dumps({
            'version': '1.0',  # Version of the logging event format.
            'log_key': order.log_key,
            'return_code': return_code,
            'output': output,
        })
        adapter.send_log(order.log_queue, body)


class RabbitMQAdapter(object):
    """Facilitate communication with captain via RabbitMQ."""

    def __init__(self, callback):
        """
        :param callback:
            Callable to run when a message is received from captain. Arguments are (adapter, body)
            where adapter is this adapter, and body is the body of the message.
        """
        self.callback = callback
        self.connection = None
        self.channel = None
        self.consumer_tag = None

        self._closing = None

    def send_log(self, log_queue, body):
        """
        Send a log event back to captain over the specified log queue.

        :param log_queue:
            Name of the queue to send the log event to.

        :param body:
            Body of the log event, typically JSON.
        """
        log.info('Sending log to log queue `{0}`.'.format(log_queue))
        def _on_queue_declared(frame):
            self.channel.basic_publish(exchange='', routing_key=log_queue, body=body)
        self.channel.queue_declare(_on_queue_declared, queue=log_queue, durable=True)

    def start(self):
        """
        Start listening for orders from captain. This method blocks until
        :func:`RabbitMQAdapter.stop` is called.
        """
        log.info('Starting RabbitMQ Adapter...')
        self.connection = self.connect()
        self.connection.ioloop.start()

    def stop(self):
        """Stop listening for orders from captain."""
        log.info('Stopping RabbitMQ Adapter...')
        self._closing = True
        if self.channel:
            self.channel.basic_cancel(self.on_consumer_cancel, self.consumer_tag)
            self.connection.ioloop.start()

    def consumer(self, channel, deliver, properties, body):
        """Pass messages to the callback registered in the constructor."""
        self.callback(self, body)

    def connect(self):
        """Create an asynchronous connection to RabbitMQ."""
        params = pika.ConnectionParameters(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            virtual_host=settings.RABBITMQ_VHOST,
            credentials=pika.credentials.PlainCredentials(
                settings.RABBITMQ_USER,
                settings.RABBITMQ_PASS
            )
        )

        return pika.SelectConnection(params, self.on_connection_open)

    def on_consumer_cancel(self, frame):
        """
        When our consumer is cancelled, close the channel, which in turn will close the connection
        and shut down the adapter.
        """
        log.info('Closing RabbitMQ channel.')
        self.channel.close()

    def on_connection_close(self, connection, reply_code, reply_text):
        """
        When the connection closes, either stop the ioloop if it was intentional, or reconnect if
        it wasn't.
        """
        self.channel = None
        if self._closing:
            log.info('Connection closed.')
            self.connection.ioloop.stop()
        else:
            log.info('Conncection closed, will reconnect in 5 seconds...')
            self.connection.add_timeout(5, self.reconnect)

    def on_channel_close(self, channel, reply_code, reply_text):
        """When the channel closes, close the connection."""
        log.info('Closing RabbitMQ connection.')
        self.connection.close()

    def on_queue_declared(self, frame):
        """Once the queue is declared, set up our consumer for that queue."""
        self.consumer_tag = self.channel.basic_consume(self.consumer, queue=settings.QUEUE_NAME,
                                                       no_ack=True)

    def on_channel_open(self, channel):
        """After creating the channel, declare the queue we want to communicate on."""
        channel.add_on_close_callback(self.on_channel_close)
        channel.queue_declare(self.on_queue_declared, queue=settings.QUEUE_NAME, durable=True)

    def on_connection_open(self, connection):
        """After we open the connection, create a channel to work with."""
        connection.add_on_close_callback(self.on_connection_close)
        self.channel = connection.channel(self.on_channel_open)

    def reconnect(self):
        """Stop the ioloop and reconnect to RabbitMQ if we aren't intentionally closed."""
        log.info('Reconnecting to RabbitMQ...')
        self.connection.ioloop.stop()
        if not self._closing:
            self.connection = self.connect()
            self.connection.ioloop.start()



def main():
    """Connect to RabbitMQ and listen for orders from the captain indefinitely."""
    adapter = RabbitMQAdapter(consume_message)

    # If a SIGINT is sent to the program, shut down the connection and exit.
    def sigint_handler(signum, frame):
        log.info('Received SIGINT, shutting down connection.')
        adapter.stop()
    signal.signal(signal.SIGINT, sigint_handler)

    log.info('Awaiting orders, sir!')
    try:
        adapter.start()
    except KeyboardInterrupt:
        adapter.stop()
        log.info('Standing down and returning to base, sir!')

if __name__ == '__main__':
    main()
