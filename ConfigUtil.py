import json
import logging
import os
from enum import Enum


class ConfigType(Enum):
    JSON = 'json'


class NullValueError(Exception):
    pass


class InvalidConfigError(Exception):
    pass


class Config(object):
    def __init__(self,
                 path):
        self._path = path

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, val):
        self._path = val

    def get_config_class_directory_name(self):
        return os.path.dirname(self.__file__)

    def get_config_class_path(self):
        return os.path.abspath(self.__file__)


class ConfigSingleton(Config):
    class __Singleton(Config):
        def __init__(self, path):
            super(Config, self).__init__(path=path)

        def __repr__(self):
            return repr(self)

    instance = None

    def __init__(self, path):
        super(ConfigSingleton, self).__init__(path=path)

        if not ConfigSingleton.instance:
            ConfigSingleton.instance = ConfigSingleton.__Singleton(path=path)
        else:
            ConfigSingleton.instance.path = path



class JsonConfig(Config):
    __DEFAULT_LOGGING_LEVEL = logging.DEBUG

    # TODO: Handle logger here?
    # TODO: Better initialization
    def __init__(self, config_path: str):
        if not config_path:
            raise NullValueError

        self.config_path = os.path.abspath(config_path)
        self.config = self.__read_config(self.config_path)

        if not self.config:
            raise InvalidConfigError('Config path is null')

    # TODO: Add support for more config file types
    def __read_config(self, config_path: str, config_type: ConfigType = None):
        # Guess config type
        if not config_type:

            if config_path.strip().lower().endswith('json'):
                config_type = ConfigType.JSON

        # JSON
        if config_type.value == ConfigType.JSON.value:
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except:
                raise InvalidConfigError('Unable to read config: {0}'.format(config_path))
        else:
            raise NotImplementedError

    def get_this_config_path(self):
        """
        Gets the path of this config file
        :return:
        :rtype:
        """
        return os.path.dirname(self.config_path)

    def get_path(self, path: str):
        """
        For when you have a relative path in the config file, but want that relative path to be relative to this
            config file's location
        :param path: relative path
        :type path:
        :return: absolute path of the relative path (path param), assuming the relative path is based on this config
        :rtype:
        """
        return os.path.join(self.get_this_config_path(), path)

    @staticmethod
    def get_logging_level(level):
        if not level:
            raise NullValueError('Unable to get logging level. It was null.')

        level = str(level).strip().lower()

        if level.isnumeric():
            level = int(level)

            if level == 50:
                return logging.CRITICAL
            elif level == 40:
                return logging.ERROR
            elif level == 30:
                return logging.WARNING
            elif level == 20:
                return logging.INFO
            elif level == 10:
                return logging.DEBUG
            elif level == 0:
                return logging.NOTSET
            else:
                raise ValueError('Logging level is not valid: {0}'.format(level))

        if level == 'critical':
            return logging.CRITICAL
        elif level == 'error':
            return logging.ERROR
        elif level == 'warning':
            return logging.WARNING
        elif level == 'info':
            return logging.INFO
        elif level == 'debug':
            return logging.DEBUG
        elif level == 'notset':
            return logging.NOTSET
        else:
            raise ValueError('Logging level is not valid: {0}'.format(level))

    # TODO: Add ability to add FileHandler
    @staticmethod
    def get_logger(logging_level, logger_name: str = None):
        if not logging_level:
            raise NullValueError('Unable to get logger. Logging level was null.')

        try:
            logging_level = Config.get_logging_level(logging_level)
        except:
            raise ValueError('Logging level was invalid: {0}'.format(logging_level))

        if not logger_name:
            logger_name = __name__

        logging.basicConfig(level=logging_level)
        # logger_name = Config.get_logger_name()

        return logging.getLogger(logger_name)

    @staticmethod
    def get_calling_method_stack():
        import inspect

        return inspect.stack()


class BasicConfig(Config):
    """
    Refer to ConfigUtil/basic_config.json for a config file template

    Has common config functionality, when you don't want to start from scratch

    The use of self.get_path() needs to be used consistently to get paths
        It converts relative paths to absolute paths
            assuming the relative path is relative to the config file's location
    """

    def __init__(self, config_path):
        super(BasicConfig, self).__init__(config_path=config_path)

    def get_logger(self):
        level = self.config['logging']['level']
        return super().get_logger(level)

    def get_logger_output_file(self):
        return self.get_path(self.config['logging']['file'])

    def get_database_name(self):
        return self.get_path(self.config['database']['name'])

    def get_database_schema_script(self):
        return self.get_path(self.config['database']['schema_script'])

    def get_database_setup_scripts(self):
        scripts = []
        for script in self.config['database']['setup_scripts']:
            scripts.append(self.get_path(script))

        return scripts
