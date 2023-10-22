from task import MyTask
from scheduler import Scheduler

if __name__ == "__main__":
    tasks = [MyTask(name=f"Task-{i}") for i in range(5)]
    scheduler = Scheduler()
    scheduler.schedule(tasks)
    scheduler.close()

