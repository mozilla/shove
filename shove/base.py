import json
import logging
import os
import re
import shlex
from collections import namedtuple
from subprocess import PIPE, Popen, STDOUT

import pika


log = logging.getLogger(__name__)
PROCFILE_LINE = re.compile(r'^([A-Za-z0-9_]+):\s*(.+)$')  # Procfile regex taken from honcho.


Order = namedtuple('Order', ('project', 'command', 'log_key', 'log_queue'))


class Shove(object):
    """
    Parses orders from Captain and executes commands based on them.

    An order contains a project name and a command to run on that project. Shove maintains a list
    of projects that it manages and paths to the directories they're stored in. When an order is
    executed, Shove looks for a procfile within the specified project and looks for a command with
    the same name as the order. If the command is found, Shove executes it in the project's
    directory and collects the output, sending it back to Captain for logging.

    To communicate with Captain, Shove needs an ``Adapter``, which handles the specifics of getting
    orders and logs between the two programs.
    """

    def __init__(self, projects, adapter):
        """
        :param projects:
            Dictionary that maps project names to the absolute path that the project's repository
            is located on this system.

        :param adapter:
            A communication adapter that Shove will use to communicate with Captain.
        """
        self.projects = projects
        self.adapter = adapter

    def start(self):
        """Start listening for commands using the communication adapter."""
        self.adapter.start(self.process_order)

    def stop(self):
        """Stop the communication adapter from listening for commands."""
        self.adapter.stop()

    def parse_order(self, order_body):
        try:
            data = json.loads(order_body)
            return Order(project=data['project'], command=data['command'], log_key=data['log_key'],
                         log_queue=data['log_queue'])
        except (KeyError, ValueError):
            log.error('Could not parse order: `{0}`'.format(order_body))
            return None

    def parse_procfile(self, path):
        """
        Parse a procfile and return a dictionary of commands in the file.

        :param path:
            Path to the procfile to parse.
        """
        commands = {}

        with open(path, 'r') as f:
            for line in f.readlines():
                match = PROCFILE_LINE.match(line)
                if match:
                    commands[match.group(1)] = shlex.split(match.group(2))
        return commands

    def execute(self, order):
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
        project_path = self.projects.get(order.project, None)
        if not project_path:
            msg = 'No project `{0}` found.'.format(order.project)
            log.warning(msg)
            return 1, msg

        procfile_path = os.path.join(project_path, 'bin', 'commands.procfile')
        try:
            commands = self.parse_procfile(procfile_path)
        except IOError as err:
            msg = 'Error loading procfile for project `{0}`: {1}'.format(order.project, err)
            log.error(msg)
            return 1, msg

        command = commands.get(order.command)
        if not command:
            msg = 'No command `{0}` found in {1}'.format(order.command, procfile_path)
            log.warning(msg)
            return 1, msg

        # Execute the order and log the result. Sends stderr to stdout so we get everything in one
        # blob.
        p = Popen(command, cwd=project_path, stdout=PIPE, stderr=STDOUT)
        output, err = p.communicate()
        log.info('Finished running {0} {1} - returned {2}'.format(order.command, command, p.returncode))
        return p.returncode, output


    def process_order(self, order_body):
        """Parse and execute an order from Captain."""
        order = self.parse_order(order_body)
        if order:
            log.info('Executing order: {0}'.format(order))
            return_code, output = self.execute(order)

            # Send the output of the command to the logging queue.
            body = json.dumps({
                'version': '1.0',  # Version of the logging event format.
                'log_key': order.log_key,
                'return_code': return_code,
                'output': output,
            })
            self.adapter.send_log(order.log_queue, body)


class RabbitMQAdapter(object):
    """Adapter for communicating with Captain via RabbitMQ."""

    def __init__(self, host, port, queue, virtual_host, username, password):
        # Connection settings.
        self.host = host
        self.port = port
        self.queue = queue
        self.virtual_host = virtual_host
        self.username = username
        self.password = password

        self.callback = None
        self.connection = None
        self.channel = None
        self.consumer_tag = None

        self._closing = None

    def send_log(self, log_queue, body):
        """
        Send a log event back to Captain over the specified log queue.

        :param log_queue:
            Name of the queue to send the log event to.

        :param body:
            Body of the log event, typically JSON.
        """
        log.info('Sending log to log queue `{0}`.'.format(log_queue))
        def _on_queue_declared(frame):
            self.channel.basic_publish(exchange='', routing_key=log_queue, body=body)
        self.channel.queue_declare(_on_queue_declared, queue=log_queue, durable=True)

    def start(self, callback):
        """
        Start listening for orders from Captain. This method blocks until
        :func:`RabbitMQAdapter.stop` is called.

        :param callback:
            Callable to run when a message is received from Captain. Arguments are (adapter, body)
            where adapter is this adapter, and body is the body of the message.
        """
        log.info('Starting RabbitMQ Adapter...')
        self.callback = callback
        self.connection = self.connect()
        self.connection.ioloop.start()

    def stop(self):
        """Stop listening for orders from Captain."""
        log.info('Stopping RabbitMQ Adapter...')
        self._closing = True
        if self.channel:
            self.channel.basic_cancel(self.on_consumer_cancel, self.consumer_tag)
            self.connection.ioloop.start()

    def consumer(self, channel, deliver, properties, body):
        """Pass messages to the callback registered in the constructor."""
        self.callback(body)

    def connect(self):
        """Create an asynchronous connection to RabbitMQ."""
        params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=pika.credentials.PlainCredentials(self.username, self.password)
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
        self.consumer_tag = self.channel.basic_consume(self.consumer, queue=self.queue,
                                                       no_ack=True)

    def on_channel_open(self, channel):
        """After creating the channel, declare the queue we want to communicate on."""
        channel.add_on_close_callback(self.on_channel_close)
        channel.queue_declare(self.on_queue_declared, queue=self.queue, durable=True)

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
