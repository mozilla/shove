import logging
import os
import sys
import time

import pika

from honcho.process import Process, ProcessManager

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

process_manager = ProcessManager()


def run(cmd):
    "Run a single command and exit"
    p = Process(cmd, stdout=sys.stdout, stderr=sys.stderr)
    p.wait()
    sys.exit(p.returncode)


def main():
    """
    connect to queue
    loop:
        command = blocking_queue_poll()
        execute(command)
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
    while(True):
        _, _, body = channel.basic_get(queue=queue, no_ack=True)
        if not body:
            print "No message returned, sleeping 5 seconds."
            time.sleep(5)
            continue
        run(body)

if __name__ == '__main__':
    main()
