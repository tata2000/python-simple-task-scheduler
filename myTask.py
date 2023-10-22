import random
import time
import logging
from config import FAILURE_PROBABILITY, MAX_SLEEP_DURATION, STUCK_PROBABILITY
from task import Task


class MyTask(Task):
    def run_task(self):
        logging.info(f"Starting task {self.name}")

        # Random sleep based on config
        sleep_duration = random.randint(1, MAX_SLEEP_DURATION)
        time.sleep(sleep_duration)

        if random.random() < STUCK_PROBABILITY:
            logging.warning(f"Task {self.name} is potentially stuck. Monitoring...")
            while not self.is_finished():  # a loop to simulate a stuck task
                time.sleep(10)

        # Random failure based on config
        if random.random() < FAILURE_PROBABILITY:
            raise Exception("Random task failure!")

        logging.info(f"Finished task {self.name} after {sleep_duration} seconds")

