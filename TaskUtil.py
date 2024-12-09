from datetime import datetime
from enum import Enum
from abc import ABCMeta, abstractmethod, abstractproperty


class TaskStateType(Enum):
    UNKNOWN = 0
    STUCK = 1
    PAUSED = 2
    NOT_STARTED = 3
    IN_PROGRESS = 4
    SUCCESSFUL = 5
    FAILURE = 6
    SHUTTING_DOWN = 7

class ITask(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def end(self):
        raise NotImplementedError

    @abstractmethod
    def _get_state(self):
        raise NotImplementedError


class Task(object):
    __metaclass__ = ABCMeta

    _start_time = None # type: datetime
    _end_time = None # type: datetime
    _state = None # type: TaskStateType

    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def end(self):
        raise NotImplementedError

    @abstractmethod
    def _get_state(self):
        raise NotImplementedError

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, val: datetime):
        if val and not self.start_time:
            self._start_time = val

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, val: datetime):
        if val and not self._end_time:
            self._end_time = val

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, val: TaskStateType):
        if not val:
            return

        if val and not type(val) != TaskStateType:
            raise TypeError

        # Set val and set end_time
        if Task.is_task_state_in_completed_state(val):
            self._end_time = datetime.utcnow()
            self._state = val


    def get_total_running_time_in_sec(self):
        if not self.start_time:
            return

        end_time = self.end_time

        if not end_time:
            end_time = datetime.utcnow()

        return (end_time - self.start_time).total_seconds()

    @staticmethod
    def is_task_state_in_completed_state(state: TaskStateType):
        if state.value in [TaskStateType.SUCCESSFUL.value,
                           TaskStateType.FAILURE.value,
                           TaskStateType.STUCK.value]:
            return True

        return False