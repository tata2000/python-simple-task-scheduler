import sqlite3
import time
from queue import Queue
import threading
import logging

from config import MAX_THREADS, DB_FILENAME
from scheduled_task import ScheduledTask


class Scheduler:
    """Scheduler to manage and run tasks concurrently."""
    def __init__(self, tasks):
        self.tasks = [ScheduledTask(f"task-{i}", task) for i, task in enumerate(tasks)]
        self.queue = Queue()
        for task in self.tasks:
            self.queue.put(task)
        self.semaphore = threading.Semaphore(MAX_THREADS)

    def worker(self):
        """Worker function to execute tasks with semaphore for concurrency control."""
        while not self.queue.empty():
            task = self.queue.get()
            with self.semaphore:
                task.execute(self.queue)
                self.semaphore.release()

    def run(self):
        self.print_summary()
        threads = []
        for _ in range(MAX_THREADS):
            thread = threading.Thread(target=self.worker)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        self.print_summary()

    def print_summary(self):
        logging.info("\n--- Task Summary ---")
        logging.info(f"{'Task Name':<15} {'Status':<10} {'Start Time':<20} {'End Time':<20} {'Retry Count'}")
        logging.info("-" * 75)
        with sqlite3.connect(DB_FILENAME) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT name, status, start_time, end_time, retry_count FROM tasks')
            for row in cursor.fetchall():
                logging.info(
                    f"{row[0]:<15} {row[1]:<10} {time.ctime(float(row[2])):<20} {time.ctime(float(row[3])):<20} {row[4]}")