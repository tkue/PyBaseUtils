from abc import ABCMeta, abstractmethod

from selenium import webdriver

from WebScrapingUtil.WebScrapingTypes import WebDriverType
from WebScrapingUtil.Config import WebScrapingConfig

from DatabaseUtils import Sqlite3Database
from NetworkUtil import NetworkUtil
from Validator import Validator


class ISession(object):
    __metaclass__ = ABCMeta

    config = ...  # type: WebScrapingConfig

    def __init__(self,
                 config: WebScrapingConfig,
                 web_driver_type: WebDriverType=None):
        self.config = config
        self.web_driver_type = web_driver_type
        self.logger = self.config.get_logger()

        self.check_connection()

        self.database = Sqlite3Database(database_path=self.config.get_database_name(),
                                        logger=self.logger,
                                        schema_script_path=self.config.get_database_schema_script(),
                                        setup_scripts=self.config.get_database_setup_scripts(),
                                        is_force_new_initialization=False)

    def _log_error(self, msg: str, error=None):
        import traceback
        if msg:
            self.logger.error(msg)

        if error:
            try:
                self.logger.error(error)
            except:
                pass

        self.logger.error(traceback.format_exc())

    def get_webdriver(self, web_driver_type: WebDriverType):
        if web_driver_type.value == WebDriverType.CHROME.value:
            opts = self.config.get_driver_options(self.web_driver_type)
            return webdriver.Chrome(executable_path=self.config.get_driver_path(self.web_driver_type),
                                    chrome_options=opts)
        else:
            raise NotImplementedError

    @staticmethod
    def get_current_ip():
        return NetworkUtil.get_public_ip()

    def is_ip_masked(self):
        original_ip = self.config.get_original_ip().strip()
        current_ip = ISession.get_current_ip()

        if not Validator.is_valid_ip_address(original_ip) or not Validator.is_valid_ip_address(current_ip):
            self.logger.critical(
                'Unable to determine if IP is masked or not because one or both IPs compared are invalid')
            self.logger.critical('Original IP: {0}\nCurrent IP: {1}'.format(original_ip, current_ip))
            raise Exception

        if original_ip != current_ip:
            return True
        else:
            self.logger.info(
                'IP address not masked:\n\tOriginal IP: {0}\n\tCurrent IP: {1}'.format(original_ip, current_ip))
            return False

    def is_can_continue_with_connection(self):
        """
        Can we continue with everything if we need to mask our IP and the IP is successfully masked?
        :return:
        :rtype:
        """

        if not self.config.get_is_need_mask_ip():
            return True

        if self.config.get_is_need_mask_ip() and self.is_ip_masked():
            return True

        return False

    def check_connection(self):
        if not self.is_can_continue_with_connection():
            self.logger.critical('IP address is not masked and needs to be. Exiting')
            exit(1)

    @abstractmethod
    def get_start_url(self):
        raise NotImplementedError

    @abstractmethod
    def start_session(self):
        raise NotImplementedError

    @abstractmethod
    def end_session(self):
        raise NotImplementedError
