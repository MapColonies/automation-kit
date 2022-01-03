"""
This module provide data validation utils testing data on mapproxy data
"""


class PycswHandler:
    """
    This class provide multiple pycsw handlers utilities for mc automation
    """

    def __init__(self, pycsw_endpoint_url):
        __pycsw_endpoint_url = pycsw_endpoint_url


def validate_pycsw(source_json_metadata, product_id=None, product_version=None):
    """
    :return: dict of result validation
    """
    res_dict = {'validation': True, 'reason': ""}
    pycsw_records = pycsw_handler.get_record_by_id(product_id, product_version, host=config.PYCSW_URL,
                                                   params=config.PYCSW_GET_RECORD_PARAMS)

    if not pycsw_records:
        return {'validation': False, 'reason': f'Records of [{product_id}] not found'}
    links = {}
    for record in pycsw_records:
        links[record['mc:productType']] = {
            record['mc:links'][0]['@scheme']: record['mc:links'][0]['#text'],
            record['mc:links'][1]['@scheme']: record['mc:links'][1]['#text'],
            record['mc:links'][2]['@scheme']: record['mc:links'][2]['#text']
        }
    if config.TEST_ENV == 'PROD':
        source_json_metadata_dic = {'metadata': source_json_metadata}
        validation_flag, err_dict = validate_pycsw_with_shape_json(pycsw_records, source_json_metadata_dic)
    else:
        validation_flag, err_dict = validate_pycsw_with_shape_json(pycsw_records, source_json_metadata)
    res_dict['validation'] = validation_flag
    res_dict['reason'] = err_dict
    return res_dict, pycsw_records, links
