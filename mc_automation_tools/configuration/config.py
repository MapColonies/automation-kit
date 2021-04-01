from mc_automation_tools import common

S3_DOWNLOAD_EXPIRATION_TIME = common.get_environment_variable("S3_DOWNLOAD_EXPIRED_TIME", 3600)
CERT_DIR = common.get_environment_variable('CERT_DIR', None)

