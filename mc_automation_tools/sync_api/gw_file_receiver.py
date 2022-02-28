"""
This module will wrap and provide api's functionality of layer spec api of sync services
"""

import json
import logging
from mc_automation_tools.configuration import config
from mc_automation_tools import base_requests, common

_log = logging.getLogger('mc_automation_tools.sync_api.gw_file_receiver')


class FileReceiver:
    __fileReceiver = 'fileReceiver'

    def __init__(self, end_point_url):
        self.__end_point_url = end_point_url

    @property
    def get_class_params(self):
        params = {
            'fileReceiver': self.__fileReceiver
        }
        return params

    # ========================================== gw file receiver api's ================================================
    def send_to_file_receiver(self, file_name, raw_data, image=True):
        """
        This method will upload layer metadata + raw data to core's storage and trigger sync job
        :param file_name: str -> resourceId [product id] + layer version [product version] -> "productId-productVersion"
        :param raw_data: str -> target of destination of synchronization
        :param image: str -> upload image or toc file -> default image [image=True] for toc [image=False]
        :return: dict -> {status_code, {"tilesCount": int}}
        """
        url = common.combine_url(self.__end_point_url, self.__fileReceiver)
        header = None
        if image:
            header = {'Content-Type': 'application/octet-stream'}
        params = {
            'filename': file_name
        }
        resp = base_requests.send_post_binary_request(url=url,
                                                      data=raw_data,
                                                      header=header,
                                                      params=params)

        status_code, content_dict = common.response_parser(resp)
        if status_code != config.ResponseCode.Ok.value:
            raise Exception(
                f'[Upload file by file receiver]:file: {file_name},\n'
                f' return with error:[{status_code}],error msg:[{content_dict}]')
        return status_code, content_dict


# todo -> samples for sync automation integration -> should be removed after integration
# fr = FileReceiver('https://gw-file-receiver-qa-sync-gw-file-receiver-route-raster-qa.apps.v0h0bdx6.eastus.aroapp.io/')
# import glob
# layer_path = '/tmp/sync_test_data/test_receive_2022_02_28T10_32_38Z'
# tiles_path_list = [f for f in glob.glob(f'{layer_path}/**/*.png', recursive=True)]
# my_tile = tiles_path_list[0]
# toc = [f for f in glob.glob(f'{layer_path}/**/*.json', recursive=True)][0]
# fr.send_to_file_receiver('test_receive_2022_02_28T10_32_38Z/4.0/OrthophotoHistory/12/4894/2772.png', open(my_tile, "rb").read())
# # fr.send_to_file_receiver('test_receive_2022_02_28T10_32_38Z/4.0/OrthophotoHistory/toc.json', json.dumps(open(toc, "rb").read()))
# fr.send_to_file_receiver('test_receive_2022_02_28T10_32_38Z/4.0/OrthophotoHistory/toc.json', open(toc,'rb').read())



