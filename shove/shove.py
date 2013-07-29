import logging
import os
import sys
import time

import pika
from honcho.process import Process, ProcessManager


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

process_manager = ProcessManager()


def blocking_queue():
    """Create up a queue connection and returns a generator of blocking reads.
    """
    params = pika.ConnectionParameters(
        host=os.environ.get("RABBITMQ_HOST"),
        port=int(os.environ.get("RABBITMQ_PORT")),
        virtual_host=os.environ.get("RABBITMQ_VHOST"),
        credentials=pika.credentials.PlainCredentials(
            os.environ.get("RABBITMQ_USER"),
            os.environ.get("RABBITMQ_PASS")
        )
    )
    queue = os.environ.get("CAP_SHOVE_QUEUE", "default")
    channel = pika.BlockingConnection(params).channel()
    while (True):
        _, _, body = channel.basic_get(queue=queue, no_ack=True)
        if not body:
            print "Nothing yet, having a quick nap."
            time.sleep(5)
            continue
        yield body


def run(cmd):
    """Run a single command and exit"""
    p = Process(cmd, stdout=sys.stdout, stderr=sys.stderr)
    p.wait()
    print "finished running %s - returned %s" % (cmd, p.returncode)


def main():
    for command in blocking_queue():
        run(command)


if __name__ == '__main__':
    main()
