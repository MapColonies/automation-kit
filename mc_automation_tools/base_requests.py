# pylint: disable=line-too-long, invalid-name
"""
This module wrapping http protocol request sending [get, post and etc.]
"""
import logging
import json
import requests
from mc_automation_tools import common
from mc_automation_tools.configuration import config

_log = logging.getLogger('automation_tools.requests')


def send_post_request(url, body={}, header=None):
    """ send http post request by providing post full url + body , header is optional, by default:content-type': 'application/json',
    "accept": "*/* """
    common.url_validator(url)
    if not header:
        header = {'content-type': 'application/json', "accept": "*/*"}
    try:
        if not config.CERT_DIR:
            resp = requests.post(url=url, data=json.dumps(body), headers=header)
        else:
            resp = requests.post(url=url, data=json.dumps(body), headers=header, verify=config.CERT_DIR)
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.text)

    except Exception as e:
        _log.error('failed get response with error: %s and error of content:', str(e))
        raise requests.exceptions.RequestException('failed on getting response data from get response with error '
                                                   'message: %s' % str(e))

    return resp


def send_put_request(url, body=[], header=None):
    common.url_validator(url)
    if not header:
        header = {'content-type': 'application/json', "accept": "*/*"}
    try:
        if not config.CERT_DIR:
            resp = requests.put(url=url, data=json.dumps(body), headers=header)
        else:
            resp = requests.put(url=url, data=json.dumps(body), headers=header, verify=config.CERT_DIR)
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.text)
    except Exception as e:
        _log.error('failed get response with error: %s and error of content:', str(e))
        raise requests.exceptions.RequestException('failed on getting response data from put response with error '
                                                   'message: %s' % str(e))
    return resp


def send_delete_request(url):
    common.url_validator(url)
    try:
        if not config.CERT_DIR:
            resp = requests.delete(url)
        else:
            resp = requests.delete(url, verify=config.CERT_DIR)
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.content)

    except Exception as e:
        _log.error('failed delete response with error: %s', str(e))
        raise requests.exceptions.RequestException("failed on getting response data from delete response with error "
                                                   "message: %s" % str(e))

    return resp


def send_get_request(url):
    """ send http get request by providing get full url """
    common.url_validator(url)
    try:
        if not config.CERT_DIR:
            resp = requests.get(url)
        else:
            resp = requests.get(url, verify=config.CERT_DIR)
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.content)

    except Exception as e:
        _log.error('failed get response with error: %s', str(e))
        raise requests.exceptions.RequestException("failed on getting response data from get response with error "
                                                   "message: %s" % str(e))

    return resp
