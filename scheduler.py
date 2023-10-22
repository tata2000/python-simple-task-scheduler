import logging
import sqlite3
import datetime
import threading

from config import RETRY_COUNT, MAX_SLEEP_DURATION, MAX_THREADS


class Scheduler:
    def __init__(self):
        self.conn = sqlite3.connect('tasks.db', check_same_thread=False)
        self._initialize_db()
        self.semaphore = threading.Semaphore(MAX_THREADS)


    def _execute_task(self, task):
        with self.semaphore:
            task_name = task.name

            with self.conn:
                cursor = self.conn.execute("SELECT status FROM tasks WHERE name=?", (task_name,))
                status = cursor.fetchone()
                if status and status[0] == 'completed':
                    logging.info(f"Task {task_name} already completed.")
                    return

            task_thread = threading.Thread(target=task.run)
            task_thread.start()
            task_thread.join(timeout=MAX_SLEEP_DURATION + 5)

            if task_thread.is_alive():
                logging.warning(f"Task {task.name} is stuck. Killing it...")
                task.remove()
                self._mark_killed(task_name)
            elif task.is_error():
                self._log_failure(task.name)
            else:
                self._mark_completed(task_name)

    def _mark_killed(self, task_name):
        """
        Mark a task as killed in the database.

        :param task_name: Unique name of the killed task.
        """
        with self.conn:
            cursor = self.conn.execute("SELECT name FROM tasks WHERE name=?", (task_name,))
            row = cursor.fetchone()

            if row:
                # Update the existing record with status 'killed'
                self.conn.execute("UPDATE tasks SET status='killed' WHERE name=?", (task_name,))
            else:
                # Insert a new record for the task with status 'killed'
                self.conn.execute("INSERT INTO tasks (name, status, retry_count) VALUES (?, 'killed', 0)", (task_name,))

            print(f"Task {task_name} was killed due to being stuck.")


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

    def _monitor_process(self, process, task, timeout):
        """Monitor a process and terminate it if it exceeds the timeout."""
        process.join(timeout=timeout)
        if process.is_alive():
            logging.warning(f"Task {task.name} exceeded its runtime. Terminating it...")
            process.terminate()

    def _print_summary(self):
        """
        Print a summary of all tasks, including their status, execution time, and any errors, in table format.
        """
        with self.conn:
            cursor = self.conn.execute("SELECT name, status, retry_count, completed_timestamp FROM tasks ORDER BY name")
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

    # def schedule(self, tasks):
    #     """
    #     Schedule a list of tasks for execution.
    #
    #     :param tasks: List of Task instances to be executed.
    #     """
    #     semaphore = multiprocessing.Semaphore(MAX_THREADS)  # Controls the number of concurrent processes
    #
    #     def worker(task):
    #         with semaphore:
    #             self._execute_task(task)
    #
    #     for task in tasks:
    #         # Use task.name as the identifier
    #         task_name = task.name
    #
    #         # Check if task was already completed or permanently failed
    #         with self.conn:
    #             cursor = self.conn.execute("SELECT status FROM tasks WHERE name=?", (task_name,))
    #             status = cursor.fetchone()
    #             if status:
    #                 if status[0] == 'completed':
    #                     logging.info(f"Task {task_name} already completed.")
    #                     continue
    #                 elif status[0] == 'permanently failed':
    #                     logging.info(f"Task {task_name} has permanently failed.")
    #                     continue
    #
    #         # Start the task in a separate process
    #         process = multiprocessing.Process(target=worker, args=(task,))
    #         process.start()
    #
    #         # Monitor the process and terminate it if it exceeds the expected runtime
    #         self._monitor_process(process, task, MAX_SLEEP_DURATION + 5)
    #
    #     logging.info("All tasks have been executed.")

    def schedule(self, tasks):
        """
        Schedule a list of tasks for execution.

        :param tasks: List of Task instances to be executed.
        """
        threads = []

        for task in tasks:
            t = threading.Thread(target=self._execute_task, args=(task,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        logging.info("All tasks have been executed.")