# pylint: disable=line-too-long, invalid-name
"""
This module wrapping http protocol request sending [get, post and etc.]
"""

import json
import logging

import requests

from mc_automation_tools import common
from mc_automation_tools.configuration import config

_log = logging.getLogger("mc_automation_tools.requests")


def send_post_binary_request(
        url,
        data=None,
        header=None,
        params=None,
):
    """
    This method will execute similar post http request execution but,
    dedicated send request with binary data (images and etc.)
    send http post request by providing post full url + body ,
    header is optional, by default:content-type': 'application/json', "accept": "*/*
    """

    if data is None:
        data = {}
    if header is None:
        header = {"content-type": "application/json", "accept": "*/*"}
    try:
        if not config.CERT_DIR:
            resp = requests.post(url=url, data=data, headers=header, params=params)
        else:
            resp = requests.post(
                url=url,
                data=data,
                headers=header,
                verify=config.CERT_DIR,
                params=params,
            )
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.text)

    except Exception as e:
        _log.error("failed get response with error: %s and error of content:", str(e))
        raise requests.exceptions.RequestException(
            "failed on getting response data from get response with error "
            "message: %s" % str(e)
        )

    return resp


def send_post_request(url, body=None, header=None, params=None, json_format=True):
    """send http post request by providing post full url + body , header is optional, by default:content-type': 'application/json',
    "accept": "*/*"""
    if body is None:
        body = {}
    common.url_validator(url)
    if not header:
        header = {"content-type": "application/json", "accept": "*/*"}
    try:
        if not config.CERT_DIR:
            if json_format:
                resp = requests.post(
                    url=url, data=json.dumps(body), headers=header, params=params, verify=False
                )
            else:
                resp = requests.post(
                    url=url, data=body, headers=header, params=params, verify=False
                )
        else:
            if json_format:
                resp = requests.post(
                    url=url, data=json.dumps(body), headers=header, verify=config.CERT_DIR, params=params,
                )
            else:
                resp = requests.post(
                    url=url, data=body, headers=header, verify=config.CERT_DIR, params=params,
                )
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.text)

    except Exception as e:
        _log.error("failed get response with error: %s and error of content:", str(e))
        raise requests.exceptions.RequestException(
            "failed on getting response data from get response with error "
            "message: %s" % str(e)
        )

    return resp


def send_get_request(url, params=None, header=None):
    """
    send http get request by providing get full url
    :param url: url to get request
    :param params: json with key-value of query params
    :headers param: if exists you can use the headers
    :return: http response data as request library returns
    """
    common.url_validator(url)
    if header is None:
        header = {"content-type": "application/json"}
    try:
        if not config.CERT_DIR:
            resp = requests.get(url, params, headers=header, verify=False)
        else:
            resp = requests.get(
                url, params, verify=config.CERT_DIR, timeout=120, headers=header
            )
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.content)

    except Exception as e:
        _log.error("failed get response with error: %s", str(e))
        raise requests.exceptions.RequestException(
            "failed on getting response data from get response with error "
            "message: %s" % str(e)
        )

    return resp


def send_put_request(url, data, header=None):
    """
    send http put request by providing put url + body
    :param url: url to get request
    :param data: json with key-value of query params
    :param header: header of request
    :return: http response data as request library returns
    """
    common.url_validator(url)
    try:
        if not header:
            header = {"content-type": "application/json", "accept": "*/*"}

        if not config.CERT_DIR:
            resp = requests.put(url, data=data, headers=header, timeout=120)
        else:
            resp = requests.put(
                url, data=data, headers=header, verify=config.CERT_DIR, timeout=120
            )
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.content)

    except Exception as e:
        _log.error("failed get response with error: %s", str(e))
        raise requests.exceptions.RequestException(
            "failed on getting response data from get response with error "
            "message: %s" % str(e)
        )

    return resp


def send_delete_request(url, params=None, data=None):
    """
    send http delete request by providing delete url + query parameters
    :param url: url to get request
    :param params: json with key-value of query params
    :return: http response data as request library returns
    """
    common.url_validator(url)
    try:
        if not config.CERT_DIR:
            if params is not None and data is not None:
                raise ValueError(
                    "Use either params or data for DELETE requests, not both."
                )

            if params:
                resp = requests.delete(url, params=params, timeout=120)
            else:
                resp = requests.delete(url, data=data, timeout=120)
            _log.debug("response code: %d", resp.status_code)
            _log.debug("response message: %s", resp.content)

            # resp = requests.delete(url, data=params, timeout=120)
        else:
            if params is not None and data is not None:
                raise ValueError(
                    "Use either params or data for DELETE requests, not both."
                )
            if params:
                resp = requests.delete(
                    url, params=params, verify=config.CERT_DIR, timeout=120
                )
            else:
                resp = requests.delete(
                    url, data=data, verify=config.CERT_DIR, timeout=120
                )
        _log.debug("response code: %d", resp.status_code)
        _log.debug("response message: %s", resp.content)

    except Exception as e:
        _log.error("failed get response with error: %s", str(e))
        raise requests.exceptions.RequestException(
            "failed on getting response data from get response with error "
            "message: %s" % str(e)
        )

    return resp
