import logging
import threading

from myTask import MyTask
from scheduler import Scheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == "__main__":
    tasks = [MyTask(name=f"Task-{i}") for i in range(20)]
    scheduler = Scheduler()
    try:
        scheduler.schedule(tasks)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        scheduler.close()