"""
This module provide data validation utils testing data on mapproxy data
"""

import json
import logging
import os
from mc_automation_tools import base_requests, common, s3storage
from mc_automation_tools.models import structs
from mc_automation_tools.configuration.config import cache_valid_value
from mc_automation_tools.common import validate_cache_control

_log = logging.getLogger("mc_automation_tools.validators.mapproxy_validator")


class MapproxyHandler:
    """
    This class provide validation utility against data on mapproxy
    """

    def __init__(
            self,
            entrypoint_url,
            tiles_storage_provide,
            grid_origin="ul",
            s3_credential=None,
            nfs_tiles_url=None,
    ):
        self.__entrypoint_url = entrypoint_url
        self.__tiles_storage_provide = tiles_storage_provide
        self.__grid_origin = grid_origin
        self.__s3_credential = s3_credential
        self.__nfs_tiles_url = nfs_tiles_url

    def validate_layer_from_pycsw(
            self,
            pycsw_records,
            product_id,
            product_version,
            layer_id,
            header=None,
            WMS_FLAG=None,
            token=None,
            tile_to_check=None,
    ):
        """
        This method will extract the url's of mapproxy and validate access to new layer
        :param pycsw_records: list[dict] -> records per given layer
        :param product_id: layer id
        :param product_version: layer version
        :param tile_to_check: str, optional A specific tile to validate for WMTS layer access.
        :return: dict -> result state + detailed reason about layers
        """
        if not pycsw_records:
            raise ValueError(f"input pycsw is empty: [{pycsw_records}]")

        links = self.extract_from_pycsw(pycsw_records)
        for product_type in links.keys():
            for li in links[product_type]:
                if li == "WMS" or li == "WMTS_KVP":
                    links[product_type][li] += f"&token={token}"
                elif li == "WMTS_BASE":
                    links[product_type][li] += f"/{product_id}-{product_type}"
                else:
                    links[product_type][li] += f"?token={token}"

        for group in links.keys():
            layer_name = "-".join([product_id, group])
            ## set state results per layer group and type
            links[group]["is_valid"] = {}

            # check that wms include the new layer on capabilities
            if os.getenv("WMS_FLAG"):
                links[group]["is_valid"][structs.MapProtocolType.WMS.value] = (
                    self.validate_wms(
                        links[group][structs.MapProtocolType.WMS.value],
                        layer_name,
                        header=header,
                        token=token,
                    )
                )
                if not links[group]["is_valid"][structs.MapProtocolType.WMS.value]:
                    _log.error(
                        f"WMS layer not found on capabilities, layer name: [{layer_name}]"
                    )

            # check that WMTS url returns the new layer on get capabilities request
            links[group]["is_valid"][structs.MapProtocolType.WMTS.value] = (
                self.validate_wmts(
                    links[group][structs.MapProtocolType.WMTS.value],
                    layer_name,
                    header=header,
                    token=token,
                )
            )
            if not links[group]["is_valid"][structs.MapProtocolType.WMTS.value]:
                _log.error(
                    f"WMTS layer not found on capabilities, layer name: [{layer_name}]"
                )
                links[group]["is_valid"][structs.MapProtocolType.WMTS_LAYER.value] = (
                    False
                )

            # check that WMTS KVP url returns the new layer on get capabilities request
            links[group]["is_valid"][structs.MapProtocolType.WMTS_KVP.value] = (
                self.validate_wmts_kvp(
                    links[group][structs.MapProtocolType.WMTS_KVP.value],
                    layer_name,
                    header=header,
                    token=token,
                )
            )

            if not links[group]["is_valid"][structs.MapProtocolType.WMTS_KVP.value]:
                _log.error(
                    f"WMTS layer not found on KVP capabilities, layer name: [{layer_name}]"
                )
                links[group]["is_valid"][structs.MapProtocolType.WMTS_KVP.value] = False

            else:
                # check that wmts include the new layer on capabilities
                wmts_capabilities = common.get_xml_as_dict(
                    links[group][structs.MapProtocolType.WMTS.value],
                    header=header,
                    token=token,
                )
                list_of_wmts_layers = [
                    layer
                    for layer in wmts_capabilities["Capabilities"]["Contents"]["Layer"]
                    if layer_name in layer["ows:Identifier"]
                ]
                if not list_of_wmts_layers:
                    raise Exception(
                        f"WMTS capabilities not found for layer: [{layer_name}]"
                    )
                wmts_tile_properties = list_of_wmts_layers[0]
                wmts_template_url = list_of_wmts_layers[0]["ResourceURL"]["@template"]
                if tile_to_check != None:
                    links[group]["is_valid"][structs.MapProtocolType.WMTS.value] = (
                        self.validate_wmts_layer(
                            wmts_template_url=wmts_template_url,
                            wmts_tile_matrix_set=wmts_tile_properties,
                            layer_name=layer_name,
                            layer_id=layer_id,
                            header=header,
                            token=token,
                            tile_to_check = tile_to_check,
                        )
                    )
                else:
                    links[group]["is_valid"][structs.MapProtocolType.WMTS.value] = (
                        self.validate_wmts_layer(
                            wmts_template_url=wmts_template_url,
                            wmts_tile_matrix_set=wmts_tile_properties,
                            layer_name=layer_name,
                            layer_id=layer_id,
                            header=header,
                            token=token,
                        )
                    )

        validation = True
        for group_name, value in links.items():
            if not all(val for key, val in value["is_valid"].items()):
                validation = False
                break
        _log.info(
            f"validation of discrete layers on mapproxy status:\n"
            f"{json.dumps(links, indent=4)}"
        )
        return {"validation": validation, "reason": links[product_type]}

    @classmethod
    def validate_wms(cls, wms_capabilities_url, layer_name, header, token=None):
        """
        This method will provide if layer exists in wms capabilities or not
        :param wms_capabilities_url: url for all wms capabilities on server (mapproxy)
        :param layer_name: orthophoto layer id
        """
        try:
            if token:
                wms_capabilities = common.get_xml_as_dict(
                    url=wms_capabilities_url, token=token, header=header
                )
            else:
                wms_capabilities = common.get_xml_as_dict(wms_capabilities_url, header)
        except Exception as e:
            _log.info(f"Failed wms validation with error: [{str(e)}]")
            raise RuntimeError(f"Failed wms validation with error: [{str(e)}]")
        exists = layer_name in [
            layer["Name"]
            for layer in wms_capabilities["WMS_Capabilities"]["Capability"]["Layer"][
                "Layer"
            ]
        ]
        return exists

    @classmethod
    def validate_wmts(cls, wmts_capabilities_url, layer_name, header, token=None):
        """
        This method will provide if layer exists in wmts capabilities or not
        :param wmts_capabilities_url: url for all wmts capabilities on server (mapproxy)
        :param layer_name: orthophoto layer id
        """

        try:
            if token:
                wmts_capabilities = common.get_xml_as_dict(wmts_capabilities_url, token)
            else:
                wmts_capabilities = common.get_xml_as_dict(
                    wmts_capabilities_url, header
                )
        except Exception as e:
            _log.info(f"Failed wmts validation with error: [{str(e)}]")
            return False

        exists = layer_name in [
            layer["ows:Title"]
            for layer in wmts_capabilities["Capabilities"]["Contents"]["Layer"]
        ]
        return exists

    @classmethod
    def validate_wmts_kvp(cls, wmts_kvp_url, layer_name, header, token=None):
        """
        This method will provide if layer exists in wmts kvp capabilities or not
        :param wmts_kvp_url: url for all wmts capabilities on server (mapproxy)
        :param layer_name: orthophoto layer id
        """

        try:
            if token:
                wmts_kvp_capabilities = common.get_xml_as_dict(wmts_kvp_url, token)
            else:
                wmts_kvp_capabilities = common.get_xml_as_dict(wmts_kvp_url, header)
        except Exception as e:
            _log.info(f"Failed wmts kvp validation with error: [{str(e)}]")
            return False

        exists = layer_name in [
            layer["ows:Title"]
            for layer in wmts_kvp_capabilities["Capabilities"]["Contents"]["Layer"]
        ]
        return exists

    def validate_wmts_layer(
            self,
            wmts_template_url,
            wmts_tile_matrix_set,
            layer_name,
            layer_id,
            header,
            token,
            tile_to_check=None,
    ):
        """
        This method will provide if wmts layer protocol provide access to tiles
        :param wmts_template_url: url struct for get tiles with wmts protocol on mapproxy
        :param wmts_tile_matrix_set: properties of matrix set
        :param layer_name: orthophoto layer id -> "<product_id>-<product_version>"
        :param tile_to_check: str, optional A specific tile to validate for WMTS layer access.

        """
        # splited_layer_name = layer_name.split("-")
        # product_id = splited_layer_name[0]
        # product_version = splited_layer_name[1]

        object_key = layer_id.split("/")[0]
        # object_key = layer_id
        try:
            # check access to random tile by wmts_layer url
            if self.__tiles_storage_provide.lower() == "s3":
                entrypoint = self.__s3_credential.get_entrypoint_url()
                access_key = self.__s3_credential.get_access_key()
                secret_key = self.__s3_credential.get_secret_key()
                bucket_name = self.__s3_credential.get_bucket_name()
                s3_conn = s3storage.S3Client(entrypoint, access_key, secret_key)
                if tile_to_check is not None: 
                    list_of_tiles = [tile_to_check]
                else:
                    list_of_tiles = s3_conn.list_folder_content(bucket_name, object_key)
            elif (
                    self.__tiles_storage_provide.lower() == "fs"
                    or self.__tiles_storage_provide.lower() == "nfs"
            ):
                path = os.path.join(self.__nfs_tiles_url, object_key)
                list_of_tiles = []
                # r=root, d=directories, f = files
                for r, d, f in os.walk(path):
                    for file in f:
                        if ".png" in file:
                            list_of_tiles.append(os.path.join(r, file))
            elif (
                    self.__tiles_storage_provide.lower() == "pv"
                    or self.__tiles_storage_provide.lower() == "pvc"
            ):
                _log.warning("pvc not implemented yet for spider tiles folder")
                list_of_tiles = []

            else:
                raise Exception(
                    f"Illegal Storage provider value type: {self.__tiles_storage_provide}"
                )

            zxy = list_of_tiles[len(list_of_tiles) - 1].split("/")[-3:]
            zxy[2] = zxy[2].split(".")[0]
            if self.__grid_origin == "ul":
                zxy[2] = str(2 ** int(zxy[0]) - 1 - int(zxy[2]))
            tile_matrix_set = wmts_tile_matrix_set["TileMatrixSetLink"]["TileMatrixSet"]
            wmts_template_url = wmts_template_url.format(
                TileMatrixSet=tile_matrix_set,
                TileMatrix=zxy[0],
                TileCol=zxy[1],
                TileRow=zxy[2],
            )  # formatted url for testing
            wmts_template_url += f"?token={token}"
            resp = base_requests.send_get_request(wmts_template_url, header=header)
            cache_header = resp.headers.get("cache-control")
            is_valid_cache_control = validate_cache_control(
                cache_control_value=cache_header, expected_max_age=cache_valid_value
            )
            url_valid = (
                    resp.status_code == structs.ResponseCode.Ok.value
                    and is_valid_cache_control["is_valid"]
            )
            cache_error = (
                is_valid_cache_control["reason"]
                if not is_valid_cache_control["is_valid"]
                else "Cache validation passed"
            )
            print(f"Cache control validation status : {cache_error}")

        except Exception as e:
            _log.info(f"Failed wmts validation with error: [{str(e)}]")
            return False

        return url_valid

    @classmethod
    def extract_from_pycsw(cls, pycsw_records):
        """
        This method generate dict of layers list from provided pycsw records list
        :param pycsw_records: dict -> record of given layer
        :return: dict -> {product_type: {protocol}}
        """
        links = {}
        links[pycsw_records["mc:productType"]] = {
            link["@scheme"]: link["#text"] for link in pycsw_records["mc:links"]
        }

        return links

    # todo -> validate if actually in use
    # def validate_new_discrete(self, pycsw_records, product_id, product_version):
    #     """
    #     This method will validate access and data on mapproxy
    #     :param pycsw_records: list[dict] -> records per given layer
    #     :param product_id: layer id
    #     :param product_version: layer version
    #     :return:
    #     """
    #     if not pycsw_records:
    #         raise ValueError(f"input pycsw is empty: [{pycsw_records}]")
    #     links = {}
    #     for records in pycsw_records:
    #         links[records["mc:productType"]] = {
    #             link["@scheme"]: link["#text"] for link in records["mc:links"]
    #         }
    #
    #     results = dict.fromkeys(list(links.keys()), dict())
    #     for link_group in list(links.keys()):
    #         results[link_group] = {k: v for k, v in links[link_group].items()}
    #
    #     for group in results.keys():
    #         if group == "Orthophoto":
    #             layer_name = "-".join([product_id, group])
    #         elif group == "OrthophotoHistory":
    #             layer_name = "-".join([product_id, product_version, group])  # layer name
    #         else:
    #             raise ValueError(
    #                 f"records type on recognize as OrthophotoHistory or Orthophoto"
    #             )
    #
    #         results[group]["is_valid"] = {}
    #         # check that wms include the new layer on capabilities
    #         wms_capabilities = common.get_xml_as_dict(results[group]["WMS"])
    #         results[group]["is_valid"]["WMS"] = layer_name in [
    #             layer["Name"]
    #             for layer in wms_capabilities["WMS_Capabilities"]["Capability"]["Layer"][
    #                 "Layer"
    #             ]
    #         ]
    #
    #         # check that wmts include the new layer on capabilities
    #         wmts_capabilities = common.get_xml_as_dict(results[group]["WMTS"])
    #         results[group]["is_valid"]["WMTS"] = layer_name in [
    #             layer["ows:Identifier"]
    #             for layer in wmts_capabilities["Capabilities"]["Contents"]["Layer"]
    #         ]
    #         wmts_layer_properties = [
    #             layer
    #             for layer in wmts_capabilities["Capabilities"]["Contents"]["Layer"]
    #             if layer_name in layer["ows:Identifier"]
    #         ]
    #
    #         # check access to random tile by wmts_layer url
    #         if (
    #                 configuration.TEST_ENV == configuration.EnvironmentTypes.QA.name
    #                 or configuration.TEST_ENV == configuration.EnvironmentTypes.DEV.name
    #         ):
    #             s3_conn = s3.S3Client(
    #                 configuration.S3_END_POINT, configuration.S3_ACCESS_KEY, configuration.S3_SECRET_KEY
    #             )
    #             list_of_tiles = s3_conn.list_folder_content(
    #                 configuration.S3_BUCKET_NAME, "/".join([product_id, product_version])
    #             )
    #         elif configuration.TEST_ENV == configuration.EnvironmentTypes.PROD.name:
    #             path = os.path.join(configuration.NFS_TILES_DIR, product_id, product_version)
    #             list_of_tiles = []
    #             # r=root, d=directories, f = files
    #             for r, d, f in os.walk(path):
    #                 for file in f:
    #                     if ".png" in file:
    #                         list_of_tiles.append(os.path.join(r, file))
    #         else:
    #             raise Exception(f"Illegal environment value type: {configuration.TEST_ENV}")
    #
    #         zxy = list_of_tiles[len(list_of_tiles) - 1].split("/")[-3:]
    #         zxy[2] = zxy[2].split(".")[0]
    #         # if self.__grid_origin == "ul"
    #         zxy[2] = str(2 ** int(zxy[0]) - 1 - int(zxy[2]))
    #
    #         tile_matrix_set = wmts_layer_properties[0]["TileMatrixSetLink"]["TileMatrixSet"]
    #         wmts_layers_url = results[group]["WMTS_LAYER"]
    #         wmts_layers_url = wmts_layers_url.format(
    #             TileMatrixSet=tile_matrix_set,
    #             TileMatrix=zxy[0],
    #             TileCol=zxy[1],
    #             TileRow=zxy[2],
    #         )  # formatted url for testing
    #         resp = base_requests.send_get_request(wmts_layers_url)
    #         results[group]["is_valid"]["WMTS_LAYER"] = (
    #                 resp.status_code == configuration.ResponseCode.Ok.value
    #         )
    #
    #     # validation iteration -> check if all URL's state is True
    #     validation = True
    #     for group_name, value in results.items():
    #         if not all(val for key, val in value["is_valid"].items()):
    #             validation = False
    #             break
    #     _log.info(f"validation of discrete layers on mapproxy status:\n" f"{results}")
    #     return {"validation": validation, "reason": results}
