import sqlite3
import time
import logging
from config import DB_FILENAME, MAX_RETRIES

class ScheduledTask:
    """Represents a scheduled task with retry logic"""
    # Constants for task statuses with associated metadata
    STATUSES = {
        'scheduled': {
            'description': 'Task is scheduled and waiting to be run',
            'retryable': True
        },
        'completed': {
            'description': 'Task has successfully completed',
            'retryable': False
        },
        'failed': {
            'description': 'Task failed but can be retried',
            'retryable': True
        },
        'permanently failed': {
            'description': 'Task failed and reached max retries',
            'retryable': False
        }
    }

    STATUSES_FOR_RETRY = [status for status, data in STATUSES.items() if data['retryable']]

    def __init__(self, task_name, task):
        self.task_name = task_name
        self.task = task
        self._status, self._retry_count = self._load_data_from_db()

    def _load_data_from_db(self):
        with sqlite3.connect(DB_FILENAME) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT status, retry_count FROM tasks WHERE name = ?', (self.task_name,))
            result = cursor.fetchone()
            if result:
                return result
            return 'scheduled', 0  # Defaults

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def retry_count(self):
        return self._retry_count

    @retry_count.setter
    def retry_count(self, value):
        self._retry_count = value

    def execute(self, queue):
        if self.status not in self.STATUS_FOR_RETRY:
            logging.info(f"Skipping {self.task_name} as its status is not retryable")
            return

        logging.info(f"Executing task: {self.task_name}")
        self.start_time = time.time()

        try:
            self.task.run()
            self.status = 'completed'
        except Exception as e:
            logging.error(f"Error occurred for {self.task_name}: {str(e)}")
            self.retry_count += 1
            if self.retry_count >= MAX_RETRIES:
                self.mark_as_permanently_failed()
            else:
                self.mark_as_failed()

        self.end_time = time.time()
        self._persist_to_db()

        # If the task failed and hasn't reached max retries, re-add to the queue
        if self.status in self.STATUS_FOR_RETRY:
            logging.info(f"Re-adding {self.task_name} to the queue for retry")
            queue.put(self)

    def mark_as_failed(self):
        self.status = 'failed'

    def mark_as_permanently_failed(self):
        self.status = 'permanently failed'

    def _persist_to_db(self):
        with sqlite3.connect(DB_FILENAME) as conn:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO tasks (name, start_time, end_time, status, retry_count)
            VALUES (?, ?, ?, ?, ?)
            ''', (self.task_name, str(self.start_time), str(self.end_time), self.status, self.retry_count))
            conn.commit()
