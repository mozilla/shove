import json
import logging
import os
import sys
from collections import namedtuple

import pika
from honcho.process import Process
from honcho.procfile import Procfile


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


try:
    from . import settings
except ImportError:
    print 'Error importing settings.py, did you copy settings.py-dist yet?'
    sys.exit(1)


Order = namedtuple('Order', ('project', 'command'))


def create_channel():
    """Create a channel for communicating with RabbitMQ."""
    params = pika.ConnectionParameters(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        virtual_host=settings.RABBITMQ_VHOST,
        credentials=pika.credentials.PlainCredentials(
            settings.RABBITMQ_USER,
            settings.RABBITMQ_PASS
        )
    )

    return pika.BlockingConnection(params).channel()


def parse_order(order_body):
    data = json.loads(order_body)
    try:
        return Order(project=data['project'], command=data['command'])
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
    """
    # Locate the procfile that lists the commands available for the requested project.
    project_path = settings.PROJECTS.get(order.project, None)
    if not project_path:
        log.warning('No project `{0}` found.'.format(order.project))
        return

    procfile_path = os.path.join(project_path, 'bin', 'commands.procfile')
    try:
        with open(procfile_path, 'r') as f:
            procfile = Procfile(f.read())
    except IOError as err:
        log.error('Error loading procfile for project `{0}`: {1}'.format(order.project, err))
        return

    command = procfile.commands.get(order.command)
    if not command:
        log.warning('No command `{0}` found in {1}'.format(order.command, procfile_path))
        return

    # Execute the order and log the result.
    p = Process(command, cwd=project_path, stdout=sys.stdout, stderr=sys.stderr)
    p.wait()
    log.info('Finished running {0} - returned {1}'.format(command, p.returncode))


def consume_message(ch, method, properties, body):
    """Consume a message from the queue."""
    order = parse_order(body)
    if order:
        log.info('Executing order: {0}'.format(order))
        execute(order)


def main():
    """Connect to RabbitMQ and listen for orders from the captain indefinitely."""
    channel = create_channel()
    channel.queue_declare(queue=settings.QUEUE_NAME, durable=True)
    channel.basic_consume(consume_message, queue=settings.QUEUE_NAME, no_ack=True)
    log.info('Awaiting orders, sir!')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info('Standing down and returning to base, sir!')
    finally:
        channel.close(reply_text='Shove standing down, sir!')


if __name__ == '__main__':
    main()
