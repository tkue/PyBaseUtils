#!/usr/bin/python3


class StringUtil(object):

    @staticmethod
    def get_boolean_from_string(val: str):

        if str(val).isnumeric():
            if int(val) == 1:
                return True
            elif int(val) == 0:
                return False
            else:
                return None

        val = str(val).strip().lower()

        if val == 'true':
            return True
        elif val == 'false':
            return False

        return None

    @staticmethod
    def isNullOrEmpty(val):
        if not val \
                or val is None \
                or str(val).strip() in ('', None) \
                or len(val) == 0:
            return True

        return False

    # Get string to compare to
    #   - simply trims and makes lowercase
    @staticmethod
    def stripToLower(s):
        if StringUtil.isNullOrEmpty(s):
            return

        return str(s).strip().lower()

    @staticmethod
    def isStringsEqual(string1, string2):
        if StringUtil.isNullOrEmpty(string1) or StringUtil.isNullOrEmpty(string2):
            return
        string1 = StringUtil.stripToLower(string1)
        string2 = StringUtil.stripToLower(string2)

        return string1 == string2

    ##
    ## Get file extension from string
    ##
    '''
        Returns the file extension
        Takes into account windows and unix paths
        Can pass in:
            file name with extension
            ||
            full file path
    
    '''

    # TODO: Implementtests created as unit test
    # @deprecated
    @staticmethod
    def getFileExtension(fileName: str, is_include_dot: bool = False):
        """
        Description:
            Returns the file extension
            Takes into account windows and unix paths
            Can pass in:
                file name with extension
                ||
                full file path
        :param fileName:
        :return:
        """
        import os

        if not fileName or not os.path.isfile(fileName):
            return

        filename = os.path.basename(fileName)
        splitname = filename.split('.')

        extension = splitname[len(splitname) - 1]

        if is_include_dot:
            return ''.join('.', extension)

        return extension

    # HACK
    # TODO
    #   - Fix
    @staticmethod
    def stripChars(s):
        import re

        regex = re.compile('^[a-zA-Z0-9]')
        return regex.sub('', s)

    @staticmethod
    def isAllHaveValues(s: str()):
        isValid = False
        for val in s:
            if StringUtil.isNullOrEmpty(val):
                isValid = False
                break
            isValid = True

        return isValid

    @staticmethod
    def isValInList(val: str, valList: str()):
        isValid = False

        val = StringUtil.stripToLower(val)
        for item in valList:
            item = StringUtil.stripToLower(item)
            if val == item:
                isValid = True
                break

        return isValid

    @staticmethod
    # @deprecated
    def parseTypeFromList(inputType, compareList):
        if StringUtil.isNullOrEmpty(inputType):
            return None
        if len(compareList) == 0:
            return None

        itemType = None

        inputType = StringUtil.stripToLower(inputType)
        for item in compareList:
            itemCompare = StringUtil.stripToLower(item)
            if inputType == itemCompare:
                itemType = item
                break

        return itemType

    @staticmethod
    def encodingDetect(s):
        import chardet
        try:
            return chardet.detect(s)
        except UnicodeDecodeError:
            return chardet.detect(s.encode('utf-8'))

    @staticmethod
    def encodingConvertUnicode(s):
        from numpy import unicode

        encoding = StringUtil.encodingDetect(s)['encoding']

        if encoding == 'utf-8':
            return unicode(s)
        else:
            return unicode(s, encoding)

    # Get ASCII equivalent if possible; drop if not
    @staticmethod
    def encodingConvertAscii(s):
        import unicodedata as ud

        # NFKD ensures non-ascii are replaced by ascii equivalents
        return ud.normalize('NFKD', str(s)).encode('ascii', 'replace')

    # Get filename with extension
    # TODO: Check if '/' and if '\\' are in file - then check platform if both there
    @staticmethod
    def get_filename_without_extension(filePath):
        """
            Gets file name if file path passed in
            Takes into account Windows and Unix-based paths:
                /path/to/file.ext
                C:\\path\\to\\file.ext

                both return: file.ext
        """
        import os

        split = os.path.split(filePath)
        filename = split[len(split) - 1]

        return filename.replace(StringUtil.getFileExtension(filename, True))

    @staticmethod
    def get_filename_with_extension(filePath):
        import os

        split = os.path.split(filePath)
        return split[len(split) - 1]

    @staticmethod
    def isListOrTuple(val):
        import builtins as b

        if type(val) in (b.list, b.tuple):
            return True

        return False

    @staticmethod
    def get_timestamp_abbr():
        """
        Get year, month, day
        :return: YYYYMMDD
        """
        from datetime import datetime
        t = datetime.now()
        return '{0}{1}{2}'.format(t.year, t.month, t.day)

    @staticmethod
    def get_timestamp():
        """
        Get year, month, day, and nanoseconds (precision=4_
        :return: YYYYHHMM-NNN
        """
        from datetime import datetime
        t = datetime.now()
        return '{0}-{1}'.format(StringUtil.get_timestamp_abbr(), str(t.microsecond)[:3])

    @staticmethod
    def get_timestamp_full():
        """
        Get: year, month, day, hour, min, sec, microsecond
        :return: YYYYMMDD_HH:MM:SS.NNN
        """
        from datetime import datetime
        t = datetime.now()
        return '{0}_{1}:{2}:{3}.{4}' \
            .format(StringUtil.get_timestamp_abbr(), t.hour, t.minute, t.second, str(t.microsecond)[:3])

    @staticmethod
    def get_encoded_value(val):
        if isNullOrEmpty(val):
            return None

        from io import StringIO

        output = StringIO
        try:
            output.write(val)
            return val
        except UnicodeDecodeError:
            return encodingConvertUnicode(val)
        finally:
            output.close()

    @staticmethod
    def strip_tolower(val: str):
        if not val:
            return
        return val.strip().lower()

    @staticmethod
    def get_compressed_str(val: str):
        """
        Description:
            Pass in a string and get the compressed, gzip value

        :val: str
            String value that is to be compressed
        :returns:
            Complete gzip-compatible binary string
        """
        if not val:
            return

        import StringIO  # FIXME: can't import?
        import gzip

        out = StringIO.StringIO()
        with gzip.GzipFile(fileobj=out, mode='w') as f:
            f.write(val)

        return out.getvalue()

    @staticmethod
    def get_all_hrefs(page_source: str):
        import re

        return re.findall(r'href=[\'"]?([^\'" >]+)', page_source)

    @staticmethod
    def remove_everything_but_numbers(val: str):
        if not val:
            return

        import re

        return int(re.sub('[^\d]', '', str(val)))

    @staticmethod
    def remove_everything_but_decimals(val: str):
        if not str:
            return

        import re

        return float(re.sub('[^(\d|\.)]', '', str(val)))

    @staticmethod
    def remove_numbers(val: str):
        if not val:
            return

        import re

        return re.sub('[\d]', '', str(val))

    @staticmethod
    def get_money_from_string(val: str):
        """
        https://stackoverflow.com/questions/2150205/can-somebody-explain-a-money-regex-that-just-checks-if-the-value-matches-some-pa
        Match dollar amounts from string
        :param val:
        :type val:
        :return:
        :rtype:
        """
        if not val:
            return

        import re

        money_pat = re.compile('|'.join([
            r'^\$?(\d*\.\d{1,2})$',     #  $.50, .50, $1.50, $.5, .5
            r'^\$?(\d+)$',              #  $500, $5, 500, 5
            r'^\$(\d+\.?)$',            #  $5.
        ]))

        val = StringUtil.strip_tolower(val)

        result = money_pat.match(val)

        if not result:
            return

        return [x for x in result.groups() if x is not None].pop()

    @staticmethod
    def get_file_name_from_path_with_extension(file_path: str):
        """
        Input:
            '/home/tom/.PyCharmCE2018.2/config/scratches/scratch_43.py'
        Output
            scratch_43
        """
        if not file_path:
            return

        import os

        return os.path.basename(os.path.splitext(file_path)[0])


class StringFormatter(object):


    """
    https://mkaz.blog/code/python-string-format-cookbook/
    """


@staticmethod
def round_to_x_decimals(val, number_of_places_to_round_to: int):
    return '{:.2f}'.format(val)


@staticmethod
def remove_decimal_places(val):
    return '{:.0f}'.format(val)
