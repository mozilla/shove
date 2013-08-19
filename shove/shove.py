import json
import logging
import os
import sys

import pika
from honcho.process import Process
from honcho.procfile import Procfile


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


def create_channel():
    """Create a channel for communicating with RabbitMQ."""
    params = pika.ConnectionParameters(
        host=os.environ.get('RABBITMQ_HOST', 'localhost'),
        port=int(os.environ.get('RABBITMQ_PORT', 5672)),
        virtual_host=os.environ.get('RABBITMQ_VHOST', '/'),
        credentials=pika.credentials.PlainCredentials(
            os.environ.get('RABBITMQ_USER', 'guest'),
            os.environ.get('RABBITMQ_PASS', 'guest')
        )
    )

    return pika.BlockingConnection(params).channel()


def parse_order(order_body):
    return json.loads(order_body)


def run(order):
    """Run a single order and exit."""
    procfile_path = os.path.join(order['project_path'], 'bin', 'commands.procfile')
    with open(procfile_path, 'r') as f:
        procfile = Procfile(f.read())

    command = procfile.commands.get(order['command'])
    if not command:
        log.warning('No command `{0}` found in {1}'.format(order['command'], procfile_path))
        return

    p = Process(command, cwd=order['project_path'], stdout=sys.stdout, stderr=sys.stderr)
    p.wait()
    log.info('Finished running {0} - returned {1}'.format(command, p.returncode))


def consume_message(ch, method, properties, body):
    """Consume a message from the queue."""
    order = parse_order(body)
    run(order)


def main():
    channel = create_channel()
    queue = os.environ.get('CAP_SHOVE_QUEUE', 'captain_shove')
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_consume(consume_message, queue=queue, no_ack=True)
    log.info('Awaiting orders, sir!')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info('Standing down and returning to base, sir!')
    finally:
        channel.close(reply_text='Shove standing down, sir!')


if __name__ == '__main__':
    main()
