from abc import ABC, abstractmethod

class Task(ABC):
    """Base Task class"""

    @abstractmethod
    def run(self):
        raise NotImplementedError("You need to implement the 'run' method")
