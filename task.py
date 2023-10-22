import threading
import random
import time
from config import FAILURE_PROBABILITY, MAX_SLEEP_DURATION

class Task:
    def __init__(self, name):
        """
        Initialize a new task with a unique name.
        
        :param name: Unique identifier for the task.
        """
        self.name = name  
        self._finished = threading.Event()
        self._error = False

    def run_task(self):
        """
        The main logic of the task. 
        This should be overridden by subclasses to define task-specific logic.
        """
        raise NotImplementedError("The run_task method should be implemented by subclasses.")
    
    def run(self):
        """
        Wrapper around the run_task to handle setting flags.
        This function manages the task execution, error handling, and status tracking.
        """
        try:
            self.run_task()
        except Exception as e:
            self._error = True
            print(f"Error in task {self.name}: {e}")
        finally:
            self._finished.set()

    def is_finished(self):
        """
        Check if the task has finished execution.

        :return: True if finished, False otherwise.
        """
        return self._finished.is_set()

    def is_error(self):
        """
        Check if the task encountered an error during execution.

        :return: True if an error occurred, False otherwise.
        """
        return self._error
    
    def remove(self):
        """
        Mark the task as finished. 
        Can be overridden by subclasses for task-specific cleanup.
        """
        self._finished.set()

class MyTask(Task):
    def run_task(self):
        """
        The main logic for MyTask.
        Includes a random sleep and a possibility of a random failure based on configuration settings.
        """
        print(f"Starting task {self.name}")

        # Random failure based on config
        if random.random() < FAILURE_PROBABILITY:
            raise Exception("Random task failure!")

        # Random sleep based on config
        sleep_duration = random.randint(1, MAX_SLEEP_DURATION)
        time.sleep(sleep_duration)

        print(f"Finished task {self.name} after {sleep_duration} seconds")

