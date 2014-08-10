import imp
import logging
import os
import signal
import socket
import sys
import uuid

from shove import HeartbeatThread, ShoveThread


log = logging.getLogger(__name__)


def main():
    """
    Connect to RabbitMQ and listen for orders from Captain indefinitely.

    There are three threads run by this function:

    1. The shove thread, which listens for orders on RabbitMQ and
       executes them, sending the results of the commands back to
       Captain.

    2. The heartbeat thread, which periodically sends a heartbeat
       message to Captain to let it know that this shove instance is
       still up and handling commands.

    3. The main thread that called this function, which simply waits for
       incoming signals and closes the other threads if a SIGINT is
       received.
    """
    # Display logging to commandline for more readable output.
    fmt = '[%(threadName)10s] %(asctime)s - %(levelname)s: %(message)s (%(filename)s:%(lineno)d)'
    logging.basicConfig(format=fmt, level=logging.INFO)

    settings = load_settings()
    adapter_kwargs = {
        'host': settings.RABBITMQ_HOST,
        'port': settings.RABBITMQ_PORT,
        'virtual_host': settings.RABBITMQ_VHOST,
        'username': settings.RABBITMQ_USER,
        'password': settings.RABBITMQ_PASS,
    }

    # Initialize the shove thread and the heartbeat thread.
    queue_name = u'shove_' + unicode(uuid.uuid4())
    shove = ShoveThread(settings.PROJECTS, queue_name, adapter_kwargs, name='shove')

    heartbeat_data = {
        'version': '1.0',  # Version of the heartbeat format.
        'routing_key': queue_name,
        'hostname': socket.gethostname(),
        'project_names': ','.join(settings.PROJECTS.keys()),
    }
    heartbeat = HeartbeatThread(settings.HEARTBEAT_DELAY, settings.HEARTBEAT_QUEUE, adapter_kwargs,
                                heartbeat_data, name='heartbeat')

    # If a SIGINT is sent to the program, stop both the threads and wait
    # for them to exit.
    main.shutting_down = False
    def sigint_handler(signum, frame):
        log.info('Received SIGINT, shutting down connection.')
        heartbeat.stop()
        shove.stop()
        heartbeat.join()
        shove.join()
        main.shutting_down = True
    signal.signal(signal.SIGINT, sigint_handler)

    # Start the threads.
    heartbeat.start()
    shove.start()

    # The main thread just waits for signals until a SIGINT is received,
    # which will set main.shutting_down and let us exit.
    log.info('Awaiting orders, sir!')
    while not main.shutting_down:
        signal.pause()
    log.info('Standing down and returning to base, sir!')


def load_settings():
    """
    Attempt to load settings from environment, and if that fails,
    fallback to a local settings file.
    """
    try:
        settings = imp.load_source('shove.settings', os.environ['SHOVE_SETTINGS_FILE'])
    except (ImportError, KeyError):
        log.warning('Failed to import settings from environment variable, falling back to local '
                    'file.')
        try:
            directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            settings = imp.load_source('shove.settings', os.path.join(directory, 'shove/settings.py'))
        except ImportError:
            log.warning('Error importing settings.py, did you copy settings.py-dist yet?')
            sys.exit(1)
    return settings


if __name__ == '__main__':
    main()
