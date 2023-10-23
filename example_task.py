import logging
import threading
import time
import random

from config import FAILURE_PROBABILITY, RANDOM_WAIT_RANGE
from task import Task


class ExampleTask(Task):
    """An example task that fails based on a probability"""
    def run(self):
        wait_seconds = random.randint(1, RANDOM_WAIT_RANGE)
        time.sleep(wait_seconds)

        if random.random() < FAILURE_PROBABILITY:
            logging.error(f"{threading.current_thread().name} failed due to configured probability.")
            raise Exception("Task failed due to configured probability.")
        logging.info(f"{threading.current_thread().name} has finished")
