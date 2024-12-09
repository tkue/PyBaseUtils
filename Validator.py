import os

from StringUtil import StringUtil


class Validator(object):

    @staticmethod
    def isValidFile(filePath: str, validExtensions):
        if StringUtil.isNullOrEmpty(filePath):
            return False
        if not os.path.exists(filePath):
            return False

        fileExt = StringUtil.getFileExtension(validExtensions)
        if fileExt in validExtensions:
            return True

        return False

    @staticmethod
    def is_logging_level(level: int):
        """
        Description:
            Checks if passed in value is a valid level from logging

        Logging Levels:
            CRITICAL	  50
            ERROR	      40
            WARNING	      30
            INFO	      20
            DEBUG	      10
            NOTSET	       0
        """
        valid_levels = (0, 10, 20, 30, 40, 50)
        if level in valid_levels:
            return True

        return False

    # TODO: Use regex instead?
    @staticmethod
    def is_valid_ip_address(ip: str):
        """
        https://stackoverflow.com/questions/11264005/using-a-regex-to-match-ip-addresses-in-python
        """

        if not ip:
            return False

        import socket

        try:
            socket.inet_aton(ip)
            return True
        except:
            return False
