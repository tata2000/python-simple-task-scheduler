import sqlite3
import time
import logging
from enum import Enum
from config import DB_FILENAME, MAX_RETRIES

class TaskSatus:
    SCHEDULED = 'scheduled'
    FAILED = 'failed'
    PERMANENTLY_FAILED = 'permanently failed'
    COMPLETED = 'completed'

class ScheduledTask:
    """Represents a scheduled task with retry logic"""
    STATUS_FOR_RETRY = [TaskSatus.SCHEDULED,TaskSatus.FAILED]

    def __init__(self, task_name, task):
        self.task_name = task_name
        self.task = task
        self._status, self._retry_count , self.start_time, self.end_time = self._load_data_from_db()
        self._persist_to_db()

    def _load_data_from_db(self):
        with sqlite3.connect(DB_FILENAME) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT status, retry_count, start_time, end_time FROM tasks WHERE name = ?', (self.task_name,))
            result = cursor.fetchone()
            if result:
                return result
            return 'scheduled', 0 , -1, -1 # Defaults

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def retry_count(self):
        return self._retry_count

    def retry_count_inc(self):
        self._retry_count += 1

    def should_retry(self):
        if self.status not in self.STATUS_FOR_RETRY:
            logging.info(f"Skipping {self.task_name} as its status is not retryable")
            return False
        return True

    def execute(self, queue):
        if self.should_retry():
            logging.info(f"Executing task: {self.task_name}")
            self.start_time = time.time()

            try:
                self.retry_count_inc()
                self.task.run()
                self.status = TaskSatus.COMPLETED

            except Exception as e:
                logging.error(f"Error occurred for {self.task_name}: {str(e)}")
                self.mark_as_failed()

            self.end_time = time.time()
            self._persist_to_db()

            # If the task failed and hasn't reached max retries, re-add to the queue
            if self.status in self.STATUS_FOR_RETRY:
                logging.info(f"Re-adding {self.task_name} to the queue for retry")
                queue.put(self)

    def mark_as_failed(self):
        if self.retry_count >= MAX_RETRIES:
            self.mark_as_permanently_failed()
        else:
            self.status = TaskSatus.FAILED

    def mark_as_permanently_failed(self):
        self.status = TaskSatus.PERMANENTLY_FAILED

    def _persist_to_db(self):
        with sqlite3.connect(DB_FILENAME) as conn:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO tasks (name, start_time, end_time, status, retry_count)
            VALUES (?, ?, ?, ?, ?)
            ''', (self.task_name, str(self.start_time), str(self.end_time), self.status, self.retry_count))
            conn.commit()
