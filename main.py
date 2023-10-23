#!/usr/bin/env python3

import logging

import config
from example_task import ExampleTask
from scheduler import Scheduler

def print_current_status(scheduler):
    """Function to print the current status of tasks"""
    logging.info("Program interrupted! Printing current status...")
    scheduler.print_summary()

if __name__ == '__main__':
    logging.info("Starting the scheduler")

    # Get the list of tasks to run
    num_tasks = config.NUMBER_OF_TASKS_TO_RUN
    tasks = [ExampleTask() for _ in range(num_tasks)]

    # Schedule and run the tasks
    scheduler = Scheduler(tasks)
    try:
        scheduler.run()
    except KeyboardInterrupt:
        print_current_status(scheduler)
