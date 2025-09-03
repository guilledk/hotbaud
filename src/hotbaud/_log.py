import logging
import time
from typing import Iterable

from colorlog import ColoredFormatter


class UTCColoredFormatter(ColoredFormatter):
    '''
    A ColoredFormatter that uses UTC for timestamps
    and formats them in ISO8601 with a trailing 'Z'.

    '''

    # switch time converter to UTC
    converter = time.gmtime

    def formatTime(self, record, datefmt=None):
        # If a custom datefmt is provided, defer to the base implementation
        if datefmt:
            return super().formatTime(record, datefmt)
        # Otherwise, produce ISO8601 UTC with 'Z'
        ct = self.converter(record.created)
        t = time.strftime('%Y-%m-%dT%H:%M:%S', ct)
        return f'{t}Z'


def setup_logging(
    loglevel: str = 'info',
    silence: Iterable[str] = ('httpx', 'hypercorn'),
) -> None:
    # silence chatty dependencies
    for noisy in silence:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    formatter = UTCColoredFormatter(
        '%(asctime)s %(log_color)s%(levelname)s%(reset)s %(name)s: %(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        },
    )

    # install single stream handler
    root = logging.getLogger()

    # avoid duplicates if called twice
    if root.handlers:
        root.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(loglevel.upper())
