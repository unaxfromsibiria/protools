import logging
from uuid import UUID

import aioredis
from aio_pika import connect_robust
from aio_pika.patterns import RPC

from protools.helpers import CacheValueManager
from protools.helpers import current_time
from protools.helpers import env_var_bool
from protools.helpers import env_var_float
from protools.helpers import env_var_int
from protools.helpers import env_var_line
from protools.helpers import env_var_list
from protools.helpers import path_to_obj
from protools.logs import DEFAULT_LOGGER_NAME
from protools.logs import SYSTEM_NAME
from protools.options import AUTH_METHOD_NAME
from protools.options import METHOD_RESULT_KEY
from protools.options import REG_METHOD_NAME
from protools.options import WorkerOptionEnum
from protools.options import service_state


class BaseProcessingWorker:
    """Base worker class.
    """
    options_enum = WorkerOptionEnum
    client = None
    logger = None
    debug = False
    redis_pool = redis_transport = redis_timeout = None
    cache_value_manager_cls = CacheValueManager
    _cache_value_manager = None

    name = None
    next_step = None

    options = None
    # registration options has format:
    # 1) as list
    # options = [
    #   WorkerOptionEnum.REDIS_TRANSPORT,
    #   WorkerOptionEnum.AUTH,
    # ]
    # this mean:
    # options = {
    #   WorkerOptionEnum.REDIS_TRANSPORT: True,
    #   WorkerOptionEnum.AUTH: True,
    # }
    # 2) as dict
    # options = {
    #   WorkerOptionEnum.REDIS_TRANSPORT: True,
    #   WorkerOptionEnum.STOP_MESSAGE: "it is fake option name",
    # }

    def __init__(self, client: str, logger: object):
        """
        """
        self.logger = logger
        self.client = client
        self.debug = env_var_bool("DEBUG")
        self._cache_value_manager = self.cache_value_manager_cls()
        if not self.name:
            if self.options_dict.get(WorkerOptionEnum.AUTH_BACKEND.value):
                self.name = AUTH_METHOD_NAME

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

    @property
    def options_dict(self) -> dict:
        """Worker options for external usage.
        """
        result = {}
        if isinstance(self.options, list):
            for field in self.options:
                if isinstance(field, str):
                    field = self.options_enum(field)

                result[field.value] = True

        elif isinstance(self.options, dict):
            for field, val in self.options.items():
                if isinstance(field, str):
                    field = self.options_enum(field)

                result[field.value] = val

        return result

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
        result = None

        if self.redis_transport:
            # getting params from redis
            key = self._cache_value_manager.params_key(event)
            call_params = await self.redis_pool.get(key)
            if call_params:
                call_params = self._cache_value_manager.value_to_params(
                    call_params
                )

            if isinstance(call_params, dict):
                call_params["event"] = event
            else:
                call_params = None
                logger.error(
                    "No params in '%s' for '%s'", key, self
                )
                result = {"error": "no params in cache"}
        else:
            call_params = params

        if call_params is None:
            return result

        try:
            result = self.processing(**call_params)
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
                if isinstance(self.next_step, str):
                    result["next_call"] = self.next_step
                elif issubclass(self.next_step, BaseProcessingWorker):
                    result["next_call"] = self.next_step.name
                else:
                    logger.warning(
                        "Next step incorrect type in %s",
                        self,
                        extra=log_extra
                    )

        if self.redis_transport:
            # save result to cache
            timeout = int(self.redis_timeout)
            cache_value = self._cache_value_manager.params_to_value(result)
            key = self._cache_value_manager.result_key(event)
            result = {
                METHOD_RESULT_KEY: key,
                "event": event.hex,
                "timeout": timeout,
            }
            await self.redis_pool.set(
                key=key, value=cache_value, expire=timeout
            )

        return result


class WorkerServer:
    """Worker server.
    """
    broker_connection = None
    client = None
    logger = None
    methods = None
    use_redis_connection = True

    reg_method_name = REG_METHOD_NAME
    default_cache_timeout = 5.0

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
            "methods": {
                method.name: method.options_dict
                for method in self.methods
            }
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

        redis_address = tuple(env_var_list("REDIS_ADDRESS"))
        if len(redis_address) == 1:
            redis_address, *_ = redis_address

        redis_transport = not env_var_bool("REDIS_DATA_TRANSPORT_OFF")
        redis_timeout = (
            env_var_float("REDIS_DEFAULT_TIMEOUT") or
            env_var_float("METHOD_WAIT_TIME") or
            self.default_cache_timeout
        )

        if redis_transport or self.use_redis_connection:
            assert redis_address, "Redis configuration exprcted."
            redis_pool = await aioredis.create_redis_pool(
                address=redis_address,
                db=env_var_int("REDIS_DB") or 0,
                maxsize=env_var_int("REDIS_POOL_SIZE") or 10,
            )
        else:
            redis_pool = None

        connection = await connect_robust(**broker_options)
        self.broker_connection = connection

        channel = await connection.channel()
        rpc = await RPC.create(channel)
        for method in self.methods:
            name = f"{method.name}__{self.client}"
            method.redis_pool = redis_pool
            method.redis_transport = redis_transport
            method.redis_timeout = redis_timeout
            await rpc.register(
                name, method.run_processing, auto_delete=True
            )
            self.logger.info("Worker method '%s' active.", name)

        result = await rpc.call(
            self.reg_method_name, kwargs=self.reg_worker_params()
        )
        if result and isinstance(result, dict):
            error = result.get("error")
            if error:
                self.logger.error(
                    "Server can't registrate methods: %s", error
                )
