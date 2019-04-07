import uuid
from enum import Enum
from enum import unique

service_state = {
    "id": uuid.uuid4()
}

REG_METHOD_NAME = "reg_worker"
STOP_METHOD_NAME = "stop_server"
AUTH_METHOD_NAME = "exec_auth"
METHOD_RESULT_KEY = "result_key"


@unique
class WorkerOptionEnum(Enum):
    """Worker options.
    """

    AUTH = "auth"
    AUTH_BACKEND = "auth_backend"
    USE_HEADERS = "headers"
    REDIS_TRANSPORT = "redis_transport"
