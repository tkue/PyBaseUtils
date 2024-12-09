import os
import hashlib
from collections import defaultdict

import StringUtil
import FileUtil


class DirectoryCollection(object):
    """
    Enumerate a directory of list of directories to get a list of:
        - paths
        - directories
        - files

    Useful in finding duplicate files/tracking file integrity
    """

    def __init__(self, paths: ()):
        self._paths = paths

    @property
    def paths(self):
        return self._paths

    @paths.setter
    def paths(self, paths):
        """
        First converts paths to a tuple, if not already a tuple
        Iterates through each value, deleting any non-existant paths
        :param paths:
        :return:
        """
        if not paths:
            raise ValueError

        try:
            paths = tuple(paths)
        except:
            raise TypeError

        valid_paths = []

        for path in paths:
            if os.path.exists(path):
                valid_paths.append(path)

        if StringUtil.isNullOrEmpty(valid_paths):
            raise ValueError

        self._paths = valid_paths

    @staticmethod
    def __enumerate_single_path(path: str):
        path_collection = []
        for rootdir, subdirs, files in os.walk(path):
            for file in files:
                path_collection.append(os.path.join(rootdir, file))

        return path_collection

    @staticmethod
    def enumerate_paths(paths):
        if not paths:
            return

        paths = (paths,)
        paths_collection = []
        for path in paths:
            for enum_path in DirectoryCollection.__enumerate_single_path(path):
                paths_collection.append(enum_path)

        return paths_collection

    @staticmethod
    def __enumerate_single_subdir(path: str):
        subdir_collection = []
        for rootdir, subdirs, files in os.walk(path):
            for subdir in subdirs:
                subdir_collection.append(subdir)

        return subdir_collection

    @staticmethod
    def enumerate_dirs(paths):
        if not paths:
            return

        paths = (paths,)
        dir_collection = []
        for path in paths:
            for enum_dirs in DirectoryCollection.__enumerate_single_subdir(path):
                dir_collection.append(enum_dirs)

        return dir_collection

    @staticmethod
    def __enumerate_single_files(path: str, is_include_nonfiles=False):
        files_collection = []
        for rootdir, subdirs, files in os.walk(path):
            for file in files:
                if not os.path.isfile(file) and not is_include_nonfiles:
                    continue
                files_collection.append(file)

        return files_collection

    @staticmethod
    def enumerate_files(paths):
        if not paths:
            return

        paths = (paths,)
        file_collection = []
        for path in paths:
            for enum_files in __enumerate_single_files(path):
                file_collection.append(enum_files)

        return file_collection

    def find_duplicates_by_size(self, is_skip_non_files=True, paths_to_check_override=[]):
        # TODO: Add ability to pass in specific paths
        if not StringUtil.isNullOrEmpty(paths_to_check_override):
            raise NotImplementedError
        """
        Description:
            Find duplicate files based solely on the size per file
        :param is_skip_non_files: bool
            If true, will skip non-files, such as symlinks
        :param paths_to_check_override:
        :return:
        """
        file_sizes = defaultdict(list)
        for path in list(self.enumerate_files()):
            if not os.path.isfile(path) and is_skip_non_files:
                continue
            file_sizes[os.path.getsize(path)].append(path)

        duplicates = [x for x in file_sizes.values() if len(x) > 1]
        return [x for x in file_sizes if x.value in duplicates]

    def find_duplicates_by_hash(self, hash_type=hashlib.md5(), block_size=2 ** 20 * 60, is_skip_non_files=True):
        file_hashs = defaultdict(list)
        for path in list(self.enumerate_files()):
            if not os.path.isfile(path) and is_skip_non_files:
                continue
            file_hashs[FileUtil.calc_hash(path, hash_type, block_size)].append(path)

        duplicates = [x for x in file_hashs.values() if len(x) > 1]
        return [x for x in file_hashs if x.value in duplicates]

    @staticmethod
    def get_files_and_hashes(path: str, is_walk: bool = False, is_abs_path: bool = False):
        if not path:
            return

        from FileUtil import FileUtil

        all_files = []
        original_os_path = os.curdir
        os.chdir(path)

        if is_walk == False:
            for f in os.listdir(path):
                # if not os.path.isfile(os.path.abspath(f)):
                #     continue

                if is_abs_path:
                    path = os.path.abspath(f)
                else:
                    path = f

            all_files.append({
                'file': path,
                'md5sum': FileUtil.md5sum(os.path.abspath(f))
            })
        else:
            for rootdir, dir, files in os.walk(path):
                for file in files:
                    # if not os.path.isfile(os.path.abspath(file)):
                    #     continue

                    full_path = os.path.abspath(os.path.join(rootdir, file))
                    if not os.path.isfile(full_path):
                        continue

                    if is_abs_path:
                        path = full_path
                    else:
                        path = file

                    all_files.append({
                        'file': path,
                        'md5sum': FileUtil.md5sum(os.path.abspath(full_path))
                    })

        return all_files

    @staticmethod
    def compare_paths(path1,
                      path2,
                      is_walk: bool=False):
        if not path1 or not path2:
            return

        _good_file_types = [str, list, tuple]

        if not type(path1) in _good_file_types or not type(path2) in _good_file_types:
            raise ValueError('Expecting string or list or tuple')


        diff_files = []

        if type(path1) == str:
            path1_files = DirectoryCollection.get_files_and_hashes(path1, is_walk, True)
        else:
            path1_files = path1

        if type(path2) == str:
            path2_files = DirectoryCollection.get_files_and_hashes(path2, is_walk, True)
        else:
            path2_files = path2


        # path1 - names
        for f1 in [x['file'] for x in path1_files]:
            if not f1:
                continue

            if not f1 or f1 not in [x['file'] for x in path2_files]:
                diff_files.append(f1)

        # path1 - md5 hash
        for f1 in [x['md5sum'] for x in path1_files]:
            if not f1:
                continue

            if f1 not in [x['md5sum'] for x in path2_files]:
                diff_files.append(f1)

        # path2 - names
        for f2 in [x['file'] for x in path2_files]:
            if not f2:
                continue

            if f2 not in [x['file'] for x in path1_files]:
                diff_files.append(f2)

        # path2 - md5 hash
        for f2 in [x['md5sum'] for x in path2_files]:
            if not f2:
                continue

            if f2 not in [x['md5sum'] for x in path1_files]:
                diff_files.append(f2)

        return diff_files

