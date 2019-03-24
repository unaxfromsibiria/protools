import logging
from uuid import UUID

from aio_pika import connect_robust
from aio_pika.patterns import RPC

from protools.helpers import current_time
from protools.helpers import env_var_bool
from protools.helpers import env_var_int
from protools.helpers import env_var_line
from protools.helpers import path_to_obj
from protools.logs import DEFAULT_LOGGER_NAME
from protools.logs import SYSTEM_NAME
from protools.options import REG_METHOD_NAME
from protools.options import service_state


class BaseProcessingWorker:
    """Base worker class.
    """
    client = None
    logger = None
    debug = False

    name = None
    next_step = None

    def __init__(self, client: str, logger: object):
        """
        """
        self.logger = logger
        self.client = client
        self.debug = env_var_bool("DEBUG")

    def __str__(self) -> str:
        if self.next_step:
            back = self.next_step
        else:
            back = "client"

        return (
            f"{self.__class__.__name__}"
            f"({self.client}): {self.name} -> {back}"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def processing(self, *args, **kwargs) -> dict:
        """Main method.
        """
        pass

    async def run_processing(self, **params):
        """RPC handler.
        """
        event = UUID(params.get("event"))
        logger = self.logger
        logger.set_event(event)
        log_extra = {
            "start_time": current_time(),
            "sys": f"{SYSTEM_NAME}.worker"
        }
        try:
            result = self.processing(**params)
        except Exception as err:
            logger.critical(
                "In method '%s' error: %s",
                self.name,
                err,
                extra=log_extra
            )
        else:
            if isinstance(result, dict):
                if self.debug:
                    logger.debug(
                        "%s result: %s",
                        self,
                        result,
                        extra=log_extra
                    )
                else:
                    log_extra["count"] = len(result)
                    logger.info(
                        "Method '%s' result",
                        self.name,
                        extra=log_extra
                    )
            else:
                logger.error(
                    "Method '%s' incorrect result '%s'",
                    self.name,
                    result.__class__,
                    extra=log_extra
                )
                result = {"error": "no result"}

            if self.next_step:
                if issubclass(self.next_step, BaseProcessingWorker):
                    result["next_call"] = self.next_step.name
                elif issubclass(self.next_step, str):
                    result["next_call"] = self.next_step
                else:
                    logger.warning(
                        "Next step incorrect type in %s",
                        self,
                        extra=log_extra
                    )

        return result


class WorkerServer:
    """Worker server.
    """
    broker_connection = None
    client = None
    logger = None
    methods = None

    reg_method_name = REG_METHOD_NAME

    def __init__(self, methods: list, capacity: int = 0, logger=None):
        """
        """
        assert methods, (
            "Methods have to be list with classes or pathes of classes."
        )

        self.client = service_state.get("id").hex
        if logger is None:
            logger = logging.getLogger(DEFAULT_LOGGER_NAME)

        self.logger = logger
        if capacity > 0:
            self.capacity = capacity
        else:
            self.capacity = env_var_int("WORLER_CAPACITY") or (
                # just a big value
                1 << 32
            )

        names = []
        self.methods = []
        for method in methods:
            if isinstance(method, str):
                method = path_to_obj(method)

            assert issubclass(method, BaseProcessingWorker), (
                f"Incorrect class '{method.__class__}' of worker."
            )
            worker = method(self.client, self.logger)
            if worker.name in names:
                self.logger.error(
                    "Duplicated method name '%s'.",
                    worker.name,
                    extra={
                        "sys": f"{SYSTEM_NAME}.worker-server"
                    }
                )
            else:
                self.methods.append(worker)
                names.append(worker.name)

        names.clear()

    def reg_worker_params(self) -> dict:
        """Registration params.
        """
        return {
            "client": self.client,
            "workers": self.capacity,
            "methods": [method.name for method in self.methods]
        }

    async def run(self):
        """Registration of methods and run processing.
        """

        broker_options = {
            field: val
            for field, val in {
                "host": env_var_line("BROKER_HOST"),
                "port": env_var_line("BROKER_PORT"),
                "login": env_var_line("BROKER_LOGIN"),
                "password": env_var_line("BROKER_PASSWORD"),
                "virtualhost": env_var_line("BROKER_VIRTUALHOST"),
            }.items() if val
        }

        connection = await connect_robust(**broker_options)
        self.broker_connection = connection
        async with connection:
            channel = await connection.channel()
            rpc = await RPC.create(channel)
            for method in self.methods:
                await rpc.register(
                    method.name, self.run_processing, auto_delete=True
                )

            result = await rpc.call(
                self.reg_method_name, kwargs=self.reg_worker_params()
            )
            if result and isinstance(result, dict):
                error = result.get("error")
                if error:
                    self.logger.error(
                        "Server can't registrate methods: %s", error
                    )

        # TODO: launch loop
