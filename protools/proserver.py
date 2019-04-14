import asyncio
import json
import random
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Iterable
from uuid import UUID

import aioredis
from aio_pika import connect_robust
from aio_pika.patterns import RPC
from aiohttp import web
from aiormq.exceptions import DeliveryError

import ujson

from .helpers import current_time
from .helpers import env_var_bool
from .helpers import env_var_float
from .helpers import env_var_int
from .helpers import env_var_line
from .helpers import env_var_list
from .helpers import get_time_uuid
from .logs import SYSTEM_NAME
from .options import AUTH_METHOD_NAME
from .options import METHOD_RESULT_KEY
from .options import REG_METHOD_NAME
from .options import STOP_METHOD_NAME
from .options import WorkerOptionEnum


class ApiHandler:
    """Json-rpc 2.0 adapter.
    """
    logger = None
    version = "2.0"
    id_as_int = True
    _server_state = None
    _rpc_call_method = None

    def __init__(
            self,
            logger: object,
            server_state: dict,
            rpc_call: Callable):
        """
        """
        # TODO: create brocker connection
        self.logger = logger
        self._server_state = server_state
        self._rpc_call_method = rpc_call

    async def call(self, method: str, params: dict, request) -> dict:
        """Select and call server method.
        """
        result = await self._rpc_call_method(method, params, request)
        return result

    async def http_handler(self, request, *args, **kwargs):
        """Parse request as json rpc request.
        """
        status_code = web.HTTPOk.status_code
        error = None
        answer = None
        event = None
        try:
            data = await request.json()
            request_id = data.get("id")
            if request_id:
                if isinstance(request_id, int):
                    request_id = f"{request_id:032x}"

                event = UUID(request_id)
            else:
                event = get_time_uuid()

            self.logger.set_event(event)
        except Exception as err:
            self.logger.error("Bad request: %s", err)
            status_code = web.HTTPBadRequest.status_code
            error = f"{err}"
        else:
            method_name = data.get("method")
            if method_name and isinstance(method_name, str):
                params = data.get("params") or {}
                if isinstance(params, dict):
                    if self._server_state.get("active"):
                        params["event"] = event.hex
                        try:
                            answer = await self.call(
                                method_name, params, request
                            )
                            assert isinstance(answer, dict), "Incorrect answer format"  # noqa
                        except Exception as err:
                            status_code = web.HTTPInternalServerError.status_code  # noqa
                            error = f"{err}"
                        else:
                            if "error" in answer:
                                error = answer["error"]
                                status_code = int(
                                    answer.get("status_code") or
                                    web.HTTPGatewayTimeout.status_code
                                )
                                answer = None
                    else:
                        status_code = web.HTTPServiceUnavailable.status_code
                        error = "Server switched to inactive status."
                else:
                    status_code = web.HTTPBadRequest.status_code
                    error = "Unsupported type of 'params'"
            else:
                status_code = web.HTTPBadRequest.status_code
                error = "Incorrect JSON-RPC request (wrong name of method)"

            # TODO: JWT for params? (need token auth)

        # TODO: big result and params save to redis (have to transfer without message broker)

        resp = {"jsonrpc": "2.0"}
        if event:
            resp["id"] = event.int if self.id_as_int else event.hex

        if answer:
            resp["result"] = answer
        else:
            resp["error"] = {"code": status_code, "message": error}

        return web.json_response(
            resp,
            status=status_code,
            # ujson has problems with 128 bit int values
            dumps=json.dumps if self.id_as_int else ujson.dumps
        )


class ProcessingServer:
    """Processing server.
    """
    debug = False
    redis_transport = True
    default_priority = 5
    shutdown_timeout = 0.5
    options = logger = None
    web_app = None
    redis_pool = None
    api_class = ApiHandler
    workers = None
    methods = None
    methods_options = None
    methods_options_enum = WorkerOptionEnum
    free_workers = None
    state = None
    _next_call_queue = None
    auth_method_name = AUTH_METHOD_NAME

    def __init__(self, logger: object):
        self.logger = logger
        self.web_app = web.Application()
        self.workers = defaultdict(int)
        self.methods = defaultdict(list)
        self.methods_options = defaultdict(dict)
        self.free_workers = defaultdict(int)
        self.wait_free_timeout = env_var_float("METHOD_WAIT_TIME") or 5.0
        self.iter_delay = env_var_float("DEFAULT_ITER_DELAY") or 0.025
        self.support_iter_delay = env_var_float("SUPPORT_ITER_DELAY") or 10.0
        self._next_call_queue = asyncio.Queue()

        redis_address = tuple(env_var_list("REDIS_ADDRESS"))
        if len(redis_address) == 1:
            redis_address, *_ = redis_address

        self.options = {
            "port": env_var_int("HTTP_PORT"),
            "host": env_var_line("HTTP_HOST"),
            "broker_host": env_var_line("BROKER_HOST"),
            "broker_port": env_var_line("BROKER_PORT"),
            "broker_login": env_var_line("BROKER_LOGIN"),
            "broker_password": env_var_line("BROKER_PASSWORD"),
            "broker_virtualhost": env_var_line("BROKER_VIRTUALHOST"),
            "redis_address": redis_address,
            "redis_db": env_var_int("REDIS_DB"),
            "redis_pool_size": env_var_int("REDIS_POOL_SIZE") or 10,
            "redis_default_timeout": (
                env_var_int("REDIS_DEFAULT_TIMEOUT") or self.wait_free_timeout
            ),
            "redis_data_transport": not env_var_bool(
                "REDIS_DATA_TRANSPORT_OFF"
                ),
        }
        self.redis_transport = self.options.get("redis_data_transport")

        for field in list(self.options.keys()):
            if self.options.get(field):
                continue
            del self.options[field]

        self.state = {
            "active": True,
            "stat": {
                "call": 0,
                "inner_call": 0,
                "errors": 0,
                "input": 0.0,
            },
        }

    def __str__(self) -> str:
        options = ",".join(
            f"{field}={value}"
            for field, value in self.options.items()
        )
        return f"{self.__class__.__name__}({options})"

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def statistic(self) -> str:
        return ", ".join(
            f"{key} = {val}"
            for key, val in self.state["stat"].items()
        )

    def cache_params_key(self, event: UUID) -> str:
        """Create cache key.
        """
        return f"proserver:params:{event.hex}"

    def cache_params_to_value(self, params: dict) -> str:
        """Return data as string to record in cache.
        """
        return ujson.dumps(params)

    def cache_value_to_params(self, data: str) -> dict:
        """Return data as string to record in cache.
        """
        return ujson.loads(data)

    async def manage(self):
        """Coroutine with support processing.
        """
        delay = self.support_iter_delay
        if delay <= 0:
            delay = 10.0
            self.logger.warning("Support delay incorrect. Set to: %f", delay)

        start_time = current_time()
        while self.state.get("active"):
            prev_call_count = self.state["stat"]["call"]
            await asyncio.sleep(delay)
            d_count = self.state["stat"]["call"] - prev_call_count
            if d_count > 0:
                # input stream (speed)
                input_stream = d_count / delay
            else:
                input_stream = 0.0

            prev_input = self.state["stat"]["input"]
            self.state["stat"]["input"] = (input_stream + prev_input) / 2

            if self.debug:
                # show inner statistic
                self.logger.debug(
                    "Current input stream",
                    extra={
                        "count": input_stream,
                        "start_time": start_time,
                        "sys": f"{SYSTEM_NAME}.statistic"
                    }
                )

        self.logger.info("Stopping manager..")

    async def stop_server(self, *args, **kwargs) -> dict:
        self.state["active"] = False

    async def reg_worker(
            self, methods: dict, client: str, workers: int) -> dict:
        """New worker registration method.
        Return result as dict {"ok": True} or (error as field in result)
        """
        try:
            client_id = UUID(client).hex
        except Exception as err:
            return {"error": f"Incorrect client id value: {err}"}

        if workers < 1:
            return {"error": "Incorrect worker value."}

        methods_names = {}
        for method in methods:
            if method and isinstance(method, str):
                methods_names[method] = methods[method]

        if not methods_names:
            return {"error": "Empty methods set."}
        if not isinstance(methods_names, dict):
            return {"error": "Workers methods format incorrect."}

        for method, options in methods_names.items():
            actual_options = {}
            for opt, opt_val in options.items():
                try:
                    enum_opt = self.methods_options_enum(opt)
                except (ValueError, KeyError, TypeError):
                    return {"error": f"Unknow options '{opt}'."}
                else:
                    actual_options[enum_opt] = opt_val

            if actual_options.get(WorkerOptionEnum.AUTH_BACKEND):
                # repalce method name
                method = self.auth_method_name
                actual_options[WorkerOptionEnum.USE_HEADERS] = True
                if WorkerOptionEnum.AUTH in actual_options:
                    # no auth for method of
                    del actual_options[WorkerOptionEnum.AUTH]

            self.logger.info(
                (
                    "New method '%s' in client '%s' (capacity: %d) "
                    "set options to: %s."
                ),
                method,
                client_id,
                workers,
                options,
                extra={
                    "sys": f"{SYSTEM_NAME}.reg-worker",
                }
            )
            self.methods[method].append(client_id)
            self.methods_options[method] = actual_options

        self.free_workers[client_id] += workers
        self.workers[client_id] += workers
        return {"ok": True}

    async def connection_handler(self):
        """
        """
        fields = (
            "host",
            "port",
            "login",
            "password",
            "virtualhost",
        )
        options = {}
        for field in fields:
            value = self.options.get(f"broker_{field}")
            if value:
                options[field] = value

        self.broker_connection = connection = await connect_robust(**options)

        self.redis_pool = await aioredis.create_redis_pool(
            address=self.options.get("redis_address"),
            db=self.options.get("redis_db"),
            maxsize=self.options.get("redis_pool_size"),
        )

        async with connection:
            channel = await connection.channel()
            rpc = await RPC.create(channel)
            self.client = rpc
            await rpc.register(
                REG_METHOD_NAME, self.reg_worker, auto_delete=True
            )
            await rpc.register(
                STOP_METHOD_NAME, self.stop_server, auto_delete=True
            )
            await self.wait_inner_call()

        self.logger.info("Broker processing spopping")

    async def wait_inner_call(self):
        """Polling of queue for next method call.
        """
        while self.state.get("active"):
            inner_method_call = await self._next_call_queue.get()
            start_time = current_time()
            next_method, params = inner_method_call
            event = params["event"]
            try:
                result = await self.call_rpc(next_method, params)
            except Exception as err:
                self.logger.error(
                    "Next method '%s' error: %s",
                    next_method,
                    err,
                    extra={
                        "event": event,
                        "start_time": start_time,
                        "sys": f"{SYSTEM_NAME}.call-method-next",
                    }
                )
                self.state["stat"]["errors"] += 1
            else:
                self.state["stat"]["inner_call"] += 1
                if self.debug:
                    try:
                        info = f"result: {result}"
                    except (ValueError, TypeError):
                        info = f"result size {len(result)}"

                    self.logger.debug(
                        "Next method '%s' %s",
                        next_method,
                        info,
                        extra={
                            "event": event,
                            "start_time": start_time,
                            "sys": f"{SYSTEM_NAME}.call-method-next",
                        }
                    )

    def advanced_handlers(self) -> list:
        """Other handlers.
        """
        return []

    def select_client(self, method: str, context: dict) -> str:
        """Select random client.
        """
        clients = self.methods.get(method)
        if clients:
            client = random.choice(clients)
            return client

        return ""

    async def auth_check(self, params: dict, request: web.Request) -> dict:
        """Main auth method.
        """
        # TODO: using auth backend and cache
        user_data = await self.call_rpc(self.auth_method_name, params, request)
        return user_data.get("user")

    async def next_method_call_reg(
            self,
            method: str,
            prev_result: dict,
            prev_method: str,
            prev_event: str,
            priority: int,
            timeout: float) -> dict:
        """Registration of next calling for current methods results.
        """
        next_event = get_time_uuid().hex
        new_params = {
            res_field: res_val
            for res_field, res_val in prev_result.items()
            if res_field not in (
                "event", "priority", "timeout", "next_call",
            )
        }
        new_params["event"] = next_event
        new_params["priority"] = priority
        new_params["timeout"] = timeout
        await self._next_call_queue.put((method, new_params))
        self.logger.info(
            "Method '%s' going to call next method '%s' with event id '%s'",
            prev_method,
            method,
            next_event,
            extra={
                "event": prev_event,
                "sys": f"{SYSTEM_NAME}.call-method-next",
            }
        )
        return {"next_event": next_event}

    async def call_rpc(
            self,
            method: str,
            params: dict,
            request: web.Request = None) -> dict:
        """Access to client connection.
        """
        if self.debug:
            self.logger.debug("Call method '%s' with: %s", method, params)

        priority = int(params.get("priority") or self.default_priority)
        timeout = float(params.get("timeout") or self.wait_free_timeout)
        event = UUID(params["event"])
        self.state["stat"]["call"] += 1
        if request is None:
            self.state["stat"]["inner_call"] += 1

        wait = True
        out_client = None
        exec_time = try_count = 0
        start_time = current_time()
        result = {}
        context = {}
        options = self.methods_options.get(method)
        if options.get(WorkerOptionEnum.USE_HEADERS):
            params["headers"] = dict(request._message.headers)

        redis_transport = self.redis_transport
        if redis_transport:
            method_params = {"event": event.hex}
            cache_value = self.cache_params_to_value(params)
        else:
            # all data send to broker
            method_params = params
            cache_value = None

        while wait:
            out_client = None
            try_count += 1
            # method options check

            if options.get(WorkerOptionEnum.AUTH):
                auth_result = await self.auth_check(params, request)
                if not auth_result:
                    wait = False
                    result = {
                        "error": "Access denied.",
                        "status_code": web.HTTPUnauthorized.status_code,
                    }
                    break

            # select and call
            client = self.select_client(method, context)
            if client:
                # check free
                free = self.free_workers.get(client) or 0
                if free > 0:
                    self.free_workers[client] -= 1
                    try:
                        # send data to cache
                        if redis_transport:
                            await self.redis_pool.set(
                                key=self.cache_params_key(event),
                                value=cache_value,
                                expire=int(timeout)
                            )

                        # call rpc client
                        result = await self.client.call(
                            f"{method}__{client}",
                            kwargs=method_params,
                            priority=priority
                        )
                        if not isinstance(result, dict):
                            raise TypeError(
                                f"Incorrect result type {result.__class__}"
                            )
                        if redis_transport:
                            result_cache_key = result.get(METHOD_RESULT_KEY)
                            if (
                                    result_cache_key and
                                    isinstance(result_cache_key, str)):
                                result = await self.redis_pool.get(
                                    result_cache_key
                                )
                                if not isinstance(result, dict):
                                    raise TypeError(f"Incorrect result in cache by key '{result_cache_key}'")  # noqa
                            else:
                                raise ValueError(f"Expected result in cache {METHOD_RESULT_KEY}")  # noqa

                    except DeliveryError as err:
                        wait = True
                        out_client = client
                    except Exception as err:
                        wait = True
                        self.logger.error(
                            "Error in client '%s' method '%s': %s",
                            client,
                            method,
                            err,
                            extra={
                                "event": event,
                                "start_time": start_time,
                                "sys": f"{SYSTEM_NAME}.call-method"
                            }
                        )
                        self.state["stat"]["errors"] += 1
                    else:
                        wait = False
                    finally:
                        self.free_workers[client] += 1
            # #
            if wait:
                await asyncio.sleep(self.iter_delay)
                exec_time = current_time() - start_time
            else:
                # result ok
                # next async step
                next_call = result.get("next_call")
                if next_call and isinstance(next_call, str):
                    result = await self.next_method_call_reg(
                        next_call, result, method, event, priority, timeout
                    )
                # current worker
                result["worker"] = client

            if wait and exec_time > timeout:
                wait = False
                result = {
                    "error": (
                        f"All workers busy (select worker timeout: {timeout})"
                    )
                }
                self.logger.warning(
                    "Select worker timeout for method '%s'",
                    method,
                    extra={
                        "event": event,
                        "start_time": start_time,
                        "sys": f"{SYSTEM_NAME}.call-method",
                        "count": try_count
                    }
                )

            if out_client:
                try:
                    del self.free_workers[out_client]
                    del self.workers[out_client]
                    self.methods[method].remove(out_client)
                except Exception as err:
                    self.logger.error(
                        err,
                        extra={
                            "event": event,
                            "start_time": start_time,
                            "sys": f"{SYSTEM_NAME}.call-method"
                        }
                    )
                    self.state["stat"]["errors"] += 1
                else:
                    self.logger.warning(
                        "Client '%s' for method '%s' is not available.",
                        client,
                        method,
                        extra={
                            "event": event,
                            "start_time": start_time,
                            "sys": f"{SYSTEM_NAME}.call-method"
                        }
                    )

        return result

    def run(self):
        """Run server.
        """
        self.debug = debug = env_var_bool("DEBUG")
        api = self.api_class(self.logger, self.state, self.call_rpc)

        self.web_app.add_routes(
            [
                web.post('/api', api.http_handler)
            ] + (
                self.advanced_handlers()
            )
        )

        asyncio.gather(
            self.manage(),
            self.connection_handler(),
            web._run_app(
                self.web_app,
                port=self.options.get("port"),
                host=self.options.get("host"),
                access_log=self.logger if debug else None,
                shutdown_timeout=self.shutdown_timeout
            )
        )
