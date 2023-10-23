# Simple Task Scheduler

This project provides a simple task scheduler that manages and runs tasks concurrently. Tasks are represented as objects and they can fail or succeed based on a configurable probability. The scheduler handles retries if a task fails and provides a summary of all tasks' statuses.

## Overview:

- **config.py**: This module loads configurations from `config.ini`. It sets up logging and the database, and also initializes various constants used throughout the project.

- **example_task.py**: Contains `ExampleTask`, a subclass of `Task`. This task fails based on a probability and showcases how one might define custom tasks.

- **main.py**: The main execution point of the scheduler. It initializes tasks, sets up the scheduler, and handles interruptions.

- **scheduled_task.py**: Represents a task that is scheduled and contains logic for retries. It interacts with the database to fetch or persist task-related data.

- **scheduler.py**: The core scheduler that manages and runs tasks concurrently. Uses threading and a queue to manage tasks.

- **task.py**: Abstract base class that represents a task. All tasks must implement the `run` method defined here.

- **task_status.py**: Enum class that defines various statuses a task can be in, along with their descriptions and whether they are retryable.

## Getting Started:

1. Ensure you have the required configurations set up in `config.ini`.
2. Run `main.py` or `python3 main.py`  to start the scheduler and execute tasks.
