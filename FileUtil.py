#!/usr/bin/python3


import os
import hashlib

from StringUtil import StringUtil


class FileUtil(object):
    """
    Description:
        Generic utilities for working with files and/or directories
        Includes functions for backing up files, enumerating directories, hashing files, etc.
    """

    @staticmethod
    def get_home_dir():
        return os.path.expanduser('~')

    @staticmethod
    def md5sum(filename):
        if not filename or not os.path.isfile(filename):
            return

        hash_md5 = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def backup_file(filename: str, backupname: str = None):
        """
        Pass in filename/path to create a backup
        Ensures the name for the backup is unique, whehter it is specifically given or not
        :param filename: str
            Name of file or path to the file
            If os.path.isfile(filename) returns False, it raises an IOException
        :param backupname:
            Optional
            Name or path of the backup file
            If this file already exists, chars will be appended to the name until it is unique
        :return:
        """
        if not os.path.isfile(filename):
            raise IOError

        import shutil

        # Let's get the full path in case this gets run under a different user from a different context
        filename = os.path.abspath(filename)

        if not backupname:
            backupname = ''.join((os.path.basename(filename),
                                  StringUtil.get_timestamp(),
                                  '~'))
        else:
            backupname = os.path.abspath(backupname)

        # Make sure we have an original name for the backup name
        while os.path.isfile(backupname):
            from datetime import datetime as dt
            usec = dt.now().microsecond
            backupname = ''.join((backupname, '.', str(id[:4])))

    @staticmethod
    def calc_crc32(file_path: str):
        if not file_path:
            return

        import binascii

        with open(file_path, 'rb') as f:
            buf = f.read()
            buf = (binascii.crc32(buf) & 0xFFFFFFFF)
            return "%08X" % buf

    # TODO: DEPRECATE
    @staticmethod
    def enumerate_files_from_paths(paths):
        """
        Description:
            Returns list of all files from a list of paths
        :param paths: tuple
            List of all paths to enumerate
        :return:
        """
        try:
            paths = list(paths)
        except TypeError:
            return

        file_collection = []
        for path in paths:
            for rootdir, subdirs, files in os.walk(path):
                for file in files:
                    path_to_add = os.path.abspath(os.path.join(rootdir, file))
                    if not os.path.isfile(path_to_add):
                        continue
                    file_collection.append(path_to_add)

        return file_collection

    @staticmethod
    def get_files_recursively(directory: str, file_extensions: ()):
        if not directory or not file_extensions:
            return

        matched_files = []  # files that have matching extension

        directory = os.path.abspath(directory)
        fileslist = os.listdir(directory)

        for filename in fileslist:
            filepath = os.path.join(directory, filename)

            if os.path.isfile(filepath):
                for ext in file_extensions:
                    if not filepath.endswith(ext):
                        continue
                matched_files.append(filepath)

        return matched_files

    @staticmethod
    def get_unique_filename(filepath: str):
        if StringUtil.isNullOrEmpty(filepath):
            return

        import os

        filepath = os.path.getabspath(filepath)

        if os.path.exists(filepath):
            counter = 0
            while os.path.exists(filepath):
                counter += 1

                basename = os.path.splitext(filepath)[0]
                extension = os.path.splitext(filepath)[1]

                filepath = '{0}_{1}'.format(basename, counter)
                filepath = ''.join(filepath, extension)

        return filepath
