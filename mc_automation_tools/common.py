# pylint: disable=line-too-long, invalid-name
"""Common utils for data parsing and manipulation"""
import os
import platform
import posixpath
import re
import json
import hashlib
import logging
import sys
import uuid
import requests
import paramiko

_log = logging.getLogger('automation_tools.common')



def check_url_exists(url, timeout=20):
    """
    This method validate if the url get request will response 200 as working url.
    If its page error the return dict will include status_code and content keys, if it connection error without
    response, it will return error_msg key.
    return result dict that contains: url_valid [boolean], status_code [integer], content [response content], error_msg [str]
    """

    resp_dict = {'url_valid': False, 'status_code': None, 'content': None, 'error_msg': None}
    try:
        request = requests.get(url,timeout=timeout)  # Here is where im getting the error
        if request.status_code == 200:
            _log.info(f'Current url: [{url}] working with status code 200')
            resp_dict['url_valid'] = True
        else:
            _log.error(f'unable connect to current url: [{url}], with status code of [{request.status_code}]')
            _log.error(f'[{url}] connection error, response content [{request.content}]')
        resp_dict['status_code'] = request.status_code
        resp_dict['content'] = request.content
    except Exception as e:
        _log.error(f'unable connect to current url: [{url}]\nwith error of [{str(e)}]')
        resp_dict['error_msg'] = str(e)
    return resp_dict


def url_validator(url):
    """ standard validation function that check if provided string is valid url"""
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    return re.match(regex, url) is not None


def combine_url(base, *args) -> str:
    """
    This method concat / combine and build new url from list parts of url
    :param base : this is the base relative uri
    :param *args : sub directories of the url
    """
    for idx in enumerate(args):
        base = posixpath.join(base, args[idx[0]])
    return base


def response_parser(resp):
    """
    This method parsing standard request response object to readable data
    :param resp: request response object dict
    :return: status code and content dict
    """
    status_code = resp.status_code
    try:
        content_dict = json.loads(resp.text)
    except Exception as e:
        content_dict = resp.text
    return status_code, content_dict


def load_file_as_bytearray(file_uri):
    """
    This method open file and return bytearray
    :param file_uri: file location
    :return: bytearray
    """
    f = open(file_uri, "rb")
    ba = bytearray(f.read())
    return ba


def generate_unique_fingerprint(bytes_array):
    """
    This method generate using md5 algo, unique fingerprint string for given bytearray
    :param bytes_array: bytes to convert
    :return: fingerprint string
    """
    res = hashlib.md5(bytes_array)
    finger_print_str = res.hexdigest()
    return finger_print_str


def get_environment_variable(name, default_val):
    # type: (str, object) -> object
    """
    Returns an environment variable in the type set by the default value.
    If environment variable is empty or cannot be converted to default_val type, function returns default_val
    Note that for boolean value either 'True' 'Yes' '1', regardless of case sensitivity are considered as True.
    """
    value = os.environ.get(name)
    if value:
        if isinstance(default_val, bool):
            value = to_bool(value, default_val)
        elif default_val is not None:
            try:
                value = type(default_val)(value)
            except ValueError:
                _log.warning('Failed to convert environment variable %s=%s to %s', name, str(value),
                             type(default_val).__name__)
                value = default_val

    else:
        value = default_val
    return value


def to_bool(string, default):
    # type: (str- bool) -> (bool)
    """
    This method convert string to bool - "True, "Yes", "1" are considered True
    """
    if string and string.strip():
        return string.strip()[0].lower() in ['1', 't', 'y']
    return default


def str_to_bytes(string):
    """
    Convert from string to bytes
    """
    if string is None:
        return None

    return string.encode('utf-8')


def bytes_to_str(string):
    """
    Convert from bytes to str.
    """
    if string is None:
        return None

    return string.decode('utf-8')


def generate_uuid():
    """
    create uuid string with uuid python's libary
    """
    return str(uuid.uuid4())

def ping_to_ip(address):
    """
    This method implements system's command to check if some machine is alive (ping)
    """
    response = os.system("ping -c 1 " + address + "> /dev/null")
    # and then check the response...
    if response == 0:
        _log.debug(f"{address} Active")
        pingstatus = True
    else:
        _log.error(f"{address} Not reachable")
        pingstatus = False

    return pingstatus

    #
    #     pass
    # def init_logger():
    #     """Init logging mechanism for entire running"""
    #     log_mode = get_environment_variable('DEBUG_LOGS', False)
    #     logger = logging.getLogger()
    #     logger.setLevel(logging.DEBUG)
    #     # create console handler with a higher log level
    #     #
    #     # if (logger.hasHandlers()):
    #     #     logger.handlers.clear()
    #
    #     ch = logging.StreamHandler()
    #     if not log_mode:
    #         ch.setLevel(logging.INFO)
    #     else:
    #         ch.setLevel(logging.DEBUG)
    #
    #     # create formatter and add it to the handlers
    #     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #     ch.setFormatter(formatter)
    #     # add the handlers to the logger
    #     logger.addHandler(ch)
