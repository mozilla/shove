import imp
import logging
import os
import signal
import sys

from shove import RabbitMQAdapter, Shove


log = logging.getLogger(__name__)


def main():
    """Connect to RabbitMQ and listen for orders from the captain indefinitely."""
    # Display logging to commandline for more readable output.
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO)

    # Attempt to load settings from environment, and if that fails, fallback to a local settings
    # file.
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

    adapter = RabbitMQAdapter(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        queue=settings.QUEUE_NAME,
        virtual_host=settings.RABBITMQ_VHOST,
        username=settings.RABBITMQ_USER,
        password=settings.RABBITMQ_PASS
    )
    shove = Shove(settings.PROJECTS, adapter)

    # If a SIGINT is sent to the program, shut down the connection and exit.
    def sigint_handler(signum, frame):
        log.info('Received SIGINT, shutting down connection.')
        shove.stop()
    signal.signal(signal.SIGINT, sigint_handler)

    log.info('Awaiting orders, sir!')
    try:
        shove.start()
    except KeyboardInterrupt:
        shove.stop()
        log.info('Standing down and returning to base, sir!')

if __name__ == '__main__':
    main()
