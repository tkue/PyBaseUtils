from TaskUtils.TaskUtils import Task, TaskStateType
from enum import Enum
from datetime import datetime
# from ebaysdk.finding import Connection as Finding
from ebaysdk.exception import ConnectionError

import asyncio


class OpenWeather(object):
    def __int__(self):
        super(OpenWeather, self).__int__()



# class EbayTaskType(Enum):
#     FINDING = 0
#
#
# class EbayTask(Task):
#     def __int__(self,
#                 app_id: str,
#                 ebay_task_type: EbayTaskType
#                 ):
#         super(EbayTask, self).__int__()
#
#         self._app_id = app_id
#         self._ebay_task_type = ebay_task_type
#
#     def start(self):
#         self.start_time = Task.get_timestamp()
#         self.state = TaskStateType.IN_PROGRESS
#
#     def end(self):
#         self.end_time = Task.get_timestamp()
#
#     def get_state(self):
#         raise NotImplementedError
#
#
# class EbayFindingTask(EbayTask):
#     __api = None
#
#     def __init__(self,
#                  app_id: str,
#                  exec_str: str):
#         super(EbayFindingTask, self).__init__(app_id=app_id,
#                                               ebay_task_type=EbayTaskType.FINDING)
#
#         self._exec_str = exec_str
#
#     @property
#     def _api(self):
#         if not self._api:
#             self.__api = self._get_api()
#
#         return self.__api
#
#     @_api.setter
#     def _api(self, val):
#         if not val:
#             return
#
#         from inspect import getmro
#         from ebaysdk.connection import BaseConnection
#
#         classes = getmro(getmro(val.__class__))
#         if BaseConnection not in classes:
#             raise ValueError
#
#         self._api = val
#
#     def _get_api(self):
#         if self._ebay_task_type.value == EbayTaskType.FINDING.value:
#             from ebaysdk.finding import Connection as Finding
#
#             return Finding(appid=self._app_id)
#         else:
#             raise NotImplementedError
#
#     def start(self):
#         super(EbayFindingTask, self).start()
#         try:
#              async with self._get_api().execute(self._exec_str) as response:
#                 if response.code() == 200:
#                     self.state = TaskStateType.SUCCESS
#                 else:
#                     self.state = TaskStateType.FAILED
#
#                 return response.dict()
#         except ConnectionError as ce:
#             self.state = TaskStateType.FAILED
#             s = 'Connection error: {}'.format(ce)
#             print(s)
#         except Exception as e:
#             self.state = TaskStateType.FAILED
#             s = 'Failed to get response: {}'.format(e)
#             print(s)
#
#     def end(self):
#         super(EbayFindingTask, self).end()
#
#     def get_state(self):
#         pass









