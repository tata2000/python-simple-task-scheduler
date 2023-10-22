import threading
import logging


class Task:
    def __init__(self, name):
        self.name = name
        self._finished = threading.Event()
        self._error = False

    def run_task(self):
        raise NotImplementedError("The run_task method should be implemented by subclasses.")

    def run(self):
        try:
            self.run_task()
        except Exception as e:
            self._error = True
            logging.error(f"Error in task {self.name}: {e}")
        finally:
            self._finished.set()

    def is_error(self):
        """
        Check if the task encountered an error during execution.

        :return: True if an error occurred, False otherwise.
        """
        return self._error

    def is_finished(self):
        """
        Check if the task has finished execution.

        :return: True if finished, False otherwise.
        """
        return self._finished.is_set()
    
    def remove(self):
        """
        Mark the task as finished. 
        Can be overridden by subclasses for task-specific cleanup.
        """
        self._finished.set()

