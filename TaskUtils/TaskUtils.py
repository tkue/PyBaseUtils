import asyncio
from enum import Enum
from datetime import datetime
from abc import ABCMeta, abstractmethod
from time import sleep

import aiohttp
import requests


class TaskStateType(Enum):
    NOT_STARTED = 0
    IN_PROGRESS = 1
    STUCK = 2
    FAILED = 3
    SUCCESS = 4
    UNKNOWN = 5
    WAITING = 6
    PARTIAL_FAILURE_OR_SUCCESS = 7


class ApiCallType(Enum):
    GET = 'GET'
    POST = 'POST'
    DELETE = 'DELETE'
    PUT = 'PUT'


class ITask(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def end(self):
        raise NotImplementedError

    @abstractmethod
    def get_state(self):
        raise NotImplementedError


class Task(object):
    __metaclass__ = ITask

    _state = None  # type: TaskStateType
    _start_time = None  # type: datetime
    _end_time = None  # type: datetime
    _instantiation_time = None  # type: datetime

    def __int__(self):
        self._initialize()

    def _initialize(self):
        self._instantiation_time = Task.get_timestamp()
        self.state = TaskStateType.NOT_STARTED

    @property
    def state(self):
        return self.get_state()

    @state.setter
    def state(self, val: TaskStateType):
        self._state = val

        # Set start time 
        if Task.is_task_in_progress_state(val):
            self.start_time = Task.get_timestamp()

        # Set start time if we are done
        if Task.is_task_in_finished_state(val):
            self.end_time = Task.get_timestamp()

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, val: datetime):
        if val and not self._start_time:
            self._start_time = val

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, val: datetime):
        if val and not self._end_time:
            self._end_time = val

    def get_total_running_time_in_sec(self):
        if self.start_time:
            start = self.start_time
        else:
            start = datetime.utcnow()

        if self.end_time:
            end = self.end_time
        else:
            end = datetime.utcnow()

        return (end - start).total_seconds()

    def is_finished(self):
        return ApiCall.is_task_in_finished_state(self.state)

    def is_success(self):
        return ApiCall.is_task_in_success_state(self.state)

    def is_failed(self):
        return ApiCall.is_task_in_failed_state(self.state)


    @staticmethod
    def is_task_in_finished_state(state: TaskStateType):
        if state.value in [
            TaskStateType.STUCK.value,
            TaskStateType.FAILED.value,
            TaskStateType.SUCCESS.value
        ]:
            return True

        return False

    @staticmethod
    def is_task_in_progress_state(state: TaskStateType):
        if state.value in [
            TaskStateType.IN_PROGRESS.value
        ]:
            return True

        return False

    @staticmethod
    def is_task_in_success_state(state: TaskStateType):
        if state.value == TaskStateType.SUCCESS.value:
            return True

        return False

    @staticmethod
    def is_task_in_failed_state(state: TaskStateType):
        if state.value in [TaskStateType.FAILED.value,
                           TaskStateType.STUCK.value,
                           TaskStateType.PARTIAL_FAILURE_OR_SUCCESS.value]:
            return True

        return False

    @staticmethod
    def get_timestamp():
        return datetime.utcnow()

    def start(self):
        self.state = TaskStateType.IN_PROGRESS

    def end(self):
        self.state = self.get_state()

    def get_state(self):
        raise NotImplementedError


class ApiCall(Task):
    _response = None  # type: requests.models.Response

    def __init__(self,
                 api_call_type: ApiCallType,
                 url: str,
                 session: aiohttp.ClientSession = None,
                 data: dict = None,
                 headers: dict = None):
        super(ApiCall, self).__init__()

        self._api_call_type = api_call_type
        self._url = url
        self._session = session
        self._data = data
        self._headers = headers

    @property
    def response(self):
        return self._response

    @response.setter
    def response(self, val: requests.models.Request):
        self._response = val
        self.state = self.get_state()

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, val: aiohttp.ClientSession):
        self._session = val

    def __do_api_call(self,
                            api_call_type: ApiCallType,
                            url: str,
                            headers: dict,
                            data: dict):
        if api_call_type.value == ApiCallType.GET.value:
            with requests.get(url=url,
                                    headers=headers,
                                    data=data) as request:
                r = request
                return r

    def get_request(self):
        if self._api_call_type.value == ApiCallType.GET.value:
            return requests.get(url=self._url,
                                headers=self._headers,
                                data=self._data)

    async def start(self):
        self.state = TaskStateType.IN_PROGRESS
        async with aiohttp.ClientSession() as session:
            if self._api_call_type.value == ApiCallType.GET.value:
                async with session.get(self._url,
                                       data=self._data,
                                       headers=self._headers) as response:
                    self.response = response
                    return await response.text()

    def end(self):
        self.end_time = Task.get_timestamp()

    def get_status_code(self):
        if self.response:
            return self.response.status

    def get_state(self):
        if self.response:
            if self.get_status_code() in [200, 201]:
                return TaskStateType.SUCCESS
            else:
                return TaskStateType.FAILED
        else:
            return TaskStateType.IN_PROGRESS


class ApiCallCollection(Task):
    _sleep_between_checking_states = 5

    def __init__(self,
                 api_calls  # type: List[ApiCall]
                ):
        super(ApiCallCollection, self).__init__()

        self._api_calls = api_calls


    @staticmethod
    def add_session_to_api_calls(api_calls: [], session: aiohttp.ClientSession):
        if not api_calls or not session:
            return

        for call in api_calls:
            call = call # type: ApiCall
            call.session = session

        return api_calls

    def start(self):
        super(ApiCallCollection, self).start()

        self.state = TaskStateType.IN_PROGRESS

        loop = asyncio.get_event_loop()
        coroutines = [call.start() for call in self._api_calls]
        loop.run_until_complete(asyncio.gather(*coroutines))

        while not self.is_finished():
            sleep(self._sleep_between_checking_states)

        if self.is_finished():
            self.end()

    def end(self):
        super(ApiCallCollection, self).end()

    def _get_successes(self):
        return [x for x in self._api_calls if x.is_success()]

    def _get_failures(self):
        return [x for x in self._api_calls if x.is_failed()]

    def get_state(self):
        for call in self._api_calls:
            call = call # type: ApiCall

            not_started = [x for x in self._api_calls if x.state.value == TaskStateType.NOT_STARTED.value]
            in_progress = [x for x in self._api_calls if x.state.value == TaskStateType.IN_PROGRESS.value]
            finished = [x for x in self._api_calls if x.is_finished()]

            if in_progress:
                return TaskStateType.IN_PROGRESS
            if not_started and not in_progress and not finished:
                return TaskStateType.NOT_STARTED
            if finished and not not_started and not in_progress:
                if self._get_failures():
                    if self._get_successes():
                        return TaskStateType.PARTIAL_FAILURE_OR_SUCCESS
                    else:
                        return TaskStateType.FAILED
                else:
                    successes = self._get_successes()
                    if successes and len(successes) == len(self._api_calls):
                        return TaskStateType.SUCCESS
                    else:
                        return TaskStateType.UNKNOWN

if __name__ == '__main__':
    api_key = ''
    base_url = 'http://api.openweathermap.org/data/2.5/weather'
    cities = [
        'Washington',
        'Tampa',
        'Arlington',
        'Seattle',
        'New York'
    ]
    urls = []

    for city in cities:
        urls.append('{}?q={}&appid={}'.format(base_url,
                                              city,
                                              api_key))

    async def get_url(url: str):
        if not url:
            return

        async with aiohttp.ClientSession() as session:
                async with session.get(url=url) as response:
                    return await response.text()

    async def get_tasks(urls: [str]):
        if not urls:
            return

        tasks = []
        for url in urls:
            tasks.append(asyncio.create_task(get_url(url)))

        return tasks



    exit(0)
    api_calls = []
    for url in urls:
        api_calls.append(ApiCall(ApiCallType.GET,
                                 url))

    api_call_collection = ApiCallCollection(api_calls)
    api_call_collection.start()
    print(api_call_collection.get_total_running_time_in_sec())

    totaltime = 0
    for call in api_call_collection._api_calls:
        call = call # type: ApiCall
        # print(call.get_total_running_time_in_sec())
        totaltime += call.get_total_running_time_in_sec()

    print(totaltime)
