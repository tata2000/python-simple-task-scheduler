from enum import Enum

class TaskStatus(Enum):
    SCHEDULED = ('scheduled', 'Task is scheduled and waiting to be run', True)
    COMPLETED = ('completed', 'Task has successfully completed', False)
    FAILED = ('failed', 'Task failed but can be retried', True)
    PERMANENTLY_FAILED = ('permanently failed', 'Task failed and reached max retries', False)

    STATUS_FOR_RETRY = [status.value for status in TaskStatus if status.retryable]

    def __new__(cls, value, description, retryable):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description
        obj.retryable = retryable
        return obj
