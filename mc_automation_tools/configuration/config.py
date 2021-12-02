import enum
from mc_automation_tools import common


class ResponseCode(enum.Enum):
    """
    Types of server responses
    """
    Ok = 200  # server return ok status
    ChangeOk = 201  # server was return ok for changing
    NoJob = 204  # No job
    ValidationErrors = 400  # bad request
    StatusNotFound = 404  # status\es not found on db
    DuplicatedError = 409  # in case of requesting package with same name already exists
    GetwayTimeOut = 504  # some server didn't respond
    ServerError = 500  # problem with error


job_ingestion_type = 'Discrete-Tiling'
S3_DOWNLOAD_EXPIRATION_TIME = common.get_environment_variable("S3_DOWNLOAD_EXPIRED_TIME", 3600)
CERT_DIR = common.get_environment_variable('CERT_DIR', None)
CERT_DIR_GQL = common.get_environment_variable('CERT_DIR_GQL', None)

JOB_TASK_QUERY = """
query jobs ($params: JobsSearchParams){
  jobs(params: $params) {
                id
                resourceId
                version
                isCleaned
                status
                reason
                type
                created
                id
    			tasks {
                id
                status
              			}
  						 }	
										}
"""

