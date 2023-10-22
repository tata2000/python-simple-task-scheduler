import sqlite3
import datetime

from task import Task
from config import RETRY_COUNT


class Scheduler:
    def __init__(self):
        """
        Initialize the Scheduler. 
        Sets up the SQLite database connection and tables.
        """
        self.conn = sqlite3.connect('tasks.db')
        self._initialize_db()

    def _initialize_db(self):
        """
        Set up the necessary tables in the SQLite database.
        """
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                name TEXT PRIMARY KEY,
                retry_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                completed_timestamp TEXT
            )
            """)

    def _execute_task(self, task):
        """
        Execute a given task.
        Checks if the task was previously completed before execution.

        :param task: Task instance to be executed.
        """
        # Use task.name as the identifier
        task_name = task.name

        # Check if task was already completed
        with self.conn:
            cursor = self.conn.execute("SELECT status FROM tasks WHERE name=?", (task_name,))
            status = cursor.fetchone()
            if status and status[0] == 'completed':
                print(f"Task {task_name} already completed.")
                return

        task.run()

        if task.is_error():
            self._log_failure(task_name)
        else:
            self._mark_completed(task_name)

    def _log_failure(self, task_name):
        """
        Log the failure of a task in the database.
        Updates the retry count for the task.

        :param task_name: Unique name of the failed task.
        """
        with self.conn:
            cursor = self.conn.execute("SELECT retry_count, status FROM tasks WHERE name=?", (task_name,))
            row = cursor.fetchone()
            if row:
                retry_count, status = row
                if status == 'completed':
                    print(f"Task {task_name} was previously completed. Not updating status.")
                    return
                # Update retry_count for failed tasks
                retry_count += 1
                if retry_count >= RETRY_COUNT:
                    self.conn.execute("UPDATE tasks SET retry_count=?, status='permanently failed' WHERE name=?", (retry_count, task_name))
                else:
                    self.conn.execute("UPDATE tasks SET retry_count=?, status='failed' WHERE name=?", (retry_count, task_name))
            else:
                # Insert new record for tasks that have never been run before with retry_count initialized to 0
                self.conn.execute("INSERT INTO tasks (name, retry_count, status) VALUES (?, 0, 'failed')", (task_name,))
            print(f"Task {task_name} failed. Logging in database.")

    def _mark_completed(self, task_name):
        """
        Mark a task as completed in the database and store the completion timestamp.

        :param task_name: Unique name of the completed task.
        """
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn:
            cursor = self.conn.execute("SELECT name FROM tasks WHERE name=?", (task_name,))
            row = cursor.fetchone()

            if row:
                # Update the existing record with status and completion timestamp
                self.conn.execute("UPDATE tasks SET status='completed', completed_timestamp=? WHERE name=?",
                                  (current_time, task_name))
            else:
                # Insert a new record for the task with status 'completed' and the completion timestamp
                self.conn.execute(
                    "INSERT INTO tasks (name, status, retry_count, completed_timestamp) VALUES (?, 'completed', 0, ?)",
                    (task_name, current_time))

            print(f"Task {task_name} completed successfully at {current_time}.")

    def _print_summary(self):
        """
        Print a summary of all tasks, including their status, execution time, and any errors, in table format.
        """
        with self.conn:
            cursor = self.conn.execute("SELECT name, status, retry_count, completed_timestamp FROM tasks")
            rows = cursor.fetchall()

            # Print table header
            print("\nTask Summary:")
            print("-----------------------------------------------------")
            print("| {:<10} | {:<10} | {:<10} | {:<20} |".format("Task Name", "Status", "Retry Count",
                                                                 "Completion Timestamp"))
            print("-----------------------------------------------------")

            # Print table rows
            for row in rows:
                task_name, status, retry_count, completed_timestamp = row
                if not completed_timestamp:
                    completed_timestamp = "N/A"
                print(
                    "| {:<10} | {:<10} | {:<10} | {:<20} |".format(task_name, status, retry_count, completed_timestamp))

            print("-----------------------------------------------------")

    def close(self):
        """
        Close the SQLite database connection and print a summary of all tasks.
        """
        self._print_summary()
        if self.conn:
            self.conn.close()

    def schedule(self, tasks):
        """
        Schedule a list of tasks for execution.

        :param tasks: List of Task instances to be executed.
        """
        for task in tasks:
            # Use task.name as the identifier
            task_name = task.name

            # Check if task was already completed or permanently failed
            with self.conn:
                cursor = self.conn.execute("SELECT status FROM tasks WHERE name=?", (task_name,))
                status = cursor.fetchone()
                if status:
                    if status[0] == 'completed':
                        print(f"Task {task_name} already completed.")
                        continue
                    elif status[0] == 'permanently failed':
                        print(f"Task {task_name} has permanently failed.")
                        continue

            # If task was neither previously completed nor permanently failed, execute it
            self._execute_task(task)
