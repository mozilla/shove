import json
import logging
import os
import re
import shlex
from collections import namedtuple
from threading import Event, Thread
from subprocess import PIPE, Popen, STDOUT

import pika


log = logging.getLogger(__name__)
PROCFILE_LINE = re.compile(r'^([A-Za-z0-9_]+):\s*(.+)$')  # Procfile regex taken from honcho.


Order = namedtuple('Order', ('project', 'command', 'log_key', 'log_queue'))


class Shove(object):
    """
    Parses orders from Captain and executes commands based on them.

    An order contains a project name and a command to run on that
    project. Shove maintains a list of projects that it manages and
    paths to the directories they're stored in. When an order is
    executed, Shove looks for a procfile within the specified project
    and looks for a command with the same name as the order. If the
    command is found, Shove executes it in the project's directory and
    collects the output, sending it back to Captain for logging.
    """

    def __init__(self, projects):
        """
        :param projects:
            Dictionary that maps project names to the absolute path that
            the project's repository is located on this system.
        """
        self.projects = projects

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
        Parse a procfile and return a dictionary of commands in the
        file.

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

        Commands are listed in a procfile within the project's
        repository. An order contains a project and a command to
        execute; if there are any issues with finding the procfile or
        the requested command within it, we log the issue and throw away
        the order.

        Commands are executed in a subprocess, but this blocks until the
        command is finished.

        :param order:
            The order to execute, in the form of an Order namedtuple.

        :returns:
            A tuple of (return_code, output). The output includes stdout
            and stderr together. If the command failed to execute,
            return_code will be 1 and the output will be an error
            message explaining the failure.
        """
        # Locate the procfile that lists the commands available for the
        # requested project.
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

        # Execute the order and log the result. Sends stderr to stdout
        # so we get everything in one blob.
        p = Popen(command, cwd=project_path, stdout=PIPE, stderr=STDOUT)
        output, err = p.communicate()
        log.info('Finished running {0} {1} - returned {2}'.format(order.command, command, p.returncode))
        return p.returncode, output


    def process_order(self, order_body):
        """
        Parse and execute an order from Captain.

        :returns:
            Tuple of (queue, body) where queue is the name of the
            logging queue to send the command log to, and body is the
            JSON-encoded body of the log message to send.
        """
        order = self.parse_order(order_body)
        if order:
            log.info('Executing order: {0}'.format(order))
            return_code, output = self.execute(order)

            # Send the output of the command to the logging queue.
            body = json.dumps({
                'version': '1.0',  # Version of the logging event format
                'log_key': order.log_key,
                'return_code': return_code,
                'output': output,
            })
            return order.log_queue, body


class ShoveThread(Thread):
    """Thread that handles incoming orders from Captain."""
    def __init__(self, projects, queue, adapter_kwargs, *args, **kwargs):
        """
        Accepts standard threading.Thread arguments in additon to the
        listed arguments.

        :param projects:
            Map of project names to their directories. See settings.py
            for more information.

        :param queue:
            Name of the queue to listen on for commands.

        :param adapter_kwargs:
            Kwargs for RabbitMQAdapter so we can connect to RabbitMQ.
        """
        super(ShoveThread, self).__init__(*args, **kwargs)
        self.projects = projects
        self.queue = queue
        self.adapter_kwargs = adapter_kwargs

    def run(self):
        # We have to create the adapter in the thread since pika doesn't
        # work across threads.
        self.adapter = RabbitMQAdapter(**self.adapter_kwargs)
        self.adapter.connect()
        shove = Shove(self.projects)

        def callback(body):
            return shove.process_order(body)

        self.adapter.listen(self.queue, callback)
        self.adapter.close()

    def stop(self):
        self.adapter.stop_listening()


class HeartbeatThread(Thread):
    """
    A thead that sends a heartbeat event to Captain on a regular
    interval notifying it of its current status.
    """
    def __init__(self, delay, queue, adapter_kwargs, data, *args, **kwargs):
        """
        Accepts standard threading.Thread arguments in additon to the
        listed arguments.

        :param delay:
            Delay in seconds between heartbeats.

        :param queue:
            Name of the queue to sent heartbeat messages to.

        :param adapter_kwargs:
            Kwargs to use for initializing a RabbitMQAdapter for
            communicating with Captain. An adapter cannot be directly
            passed in as Pika is not safe across threads.

        :param data:
            Data to send in the body of the heartbeat message. Must be
            able to be encoded via json.dumps.
        """
        super(HeartbeatThread, self).__init__(*args, **kwargs)
        self.delay = delay
        self.queue = queue
        self.adapter_kwargs = adapter_kwargs
        self.message = json.dumps(data)

        self.close_event = Event()

    def run(self):
        adapter = RabbitMQAdapter(**self.adapter_kwargs)
        adapter.connect()

        # Send initial message at least once.
        adapter.send_message(self.queue, self.message)

        while not self.close_event.wait(self.delay):
            adapter.send_message(self.queue, self.message)

        adapter.close()

    def stop(self):
        self.close_event.set()


class RabbitMQAdapter(object):
    """
    Adapter for communicating with Captain via RabbitMQ that blocks
    instead of being asynchronous.
    """
    def __init__(self, host, port, virtual_host, username, password):
        # Connection settings.
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password

        self.connection = None
        self.channel = None

    def send_message(self, queue, body):
        """
        Send a message back to Captain over the specified queue.

        :param queue:
            Name of the queue to send the message to.

        :param body:
            Body of the message, typically JSON.
        """
        log.info('Sending message to queue `{0}`.'.format(queue))
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_publish(exchange='', routing_key=queue, body=body)

    def connect(self):
        """Create a blocking connection to RabbitMQ."""
        params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=pika.credentials.PlainCredentials(self.username, self.password),
            heartbeat_interval=600,
        )

        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

    def close(self):
        """Close the connection to RabbitMQ."""
        self.channel.close()
        self.connection.close()

    def listen(self, queue, callback):
        """
        Listen on the specified queue, passing the body of recieved
        messages to the callback. Blocks until disconnected or
        RabbitMQAdapter.stop_listening is called.
        """
        self.queue = queue
        self.channel.queue_declare(queue=queue, durable=True)

        def consume(channel, deliver, properties, body):
            result = callback(body)
            if result:
                queue, body = result
                channel.basic_publish(exchange='', routing_key=queue, body=body)

        self.channel.basic_consume(consume, queue, no_ack=True)
        self.channel.start_consuming()

    def stop_listening(self):
        """
        Stop listening for messages. Any currently-active
        RabbitMQAdapter.listen calls will return.
        """
        self.channel.stop_consuming()
        self.channel.queue_delete(self.queue)
        self.queue = None

