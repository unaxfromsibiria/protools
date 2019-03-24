import asyncio
import json
import random
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Iterable
from uuid import UUID

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
from .helpers import get_time_uuid
from .logs import SYSTEM_NAME


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
                                answer = None
                                status_code = web.HTTPGatewayTimeout.status_code  # noqa
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

        # TODO: check method in registr
        # TODO: registr for methods and workers
        # TODO: rpc call
        # TODO: check result and call next method
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
    default_priority = 5
    shutdown_timeout = 0.5
    options = logger = None
    web_app = None
    api_class = ApiHandler
    workers = None
    methods = None
    free_workers = None
    state = None
    next_call_queue = None

    def __init__(self, logger: object):
        self.options = {
            "port": env_var_int("HTTP_PORT"),
            "host": env_var_line("HTTP_HOST"),
            "broker_host": env_var_line("BROKER_HOST"),
            "broker_port": env_var_line("BROKER_PORT"),
            "broker_login": env_var_line("BROKER_LOGIN"),
            "broker_password": env_var_line("BROKER_PASSWORD"),
            "broker_virtualhost": env_var_line("BROKER_VIRTUALHOST"),
        }
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
        self.logger = logger
        self.web_app = web.Application()
        self.workers = defaultdict(int)
        self.methods = defaultdict(list)
        self.free_workers = defaultdict(int)
        self.wait_free_timeout = env_var_float("METHOD_WAIT_TIME") or 5.0
        self.iter_delay = env_var_float("DEFAULT_ITER_DELAY") or 0.025
        self.support_iter_delay = env_var_float("SUPPORT_ITER_DELAY") or 10.0
        self.next_call_queue = asyncio.Queue()

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
                    extra={"count": input_stream, "start_time": start_time}
                )

        self.logger.info("Stopping manager..")

    async def stop_server(self, *args, **kwargs) -> dict:
        self.state["active"] = False

    async def reg_worker(
            self, methods: Iterable, client: str, workers: int) -> dict:
        """New worker registration method.
        Return result as dict {"ok": True} or (error as field in result)
        """
        try:
            client_id = UUID(client)
        except Exception as err:
            return {"error": f"Incorrect client id value: {err}"}

        if workers < 1:
            return {"error": "Incorrect worker value."}

        methods_names = set()
        for method in methods:
            if method and isinstance(method, str):
                methods_names.add(method)

        if not methods_names:
            return {"error": "Empty methods list."}

        for method in methods_names:
            self.logger.info(
                "New method '%s' in client '%s' (capacity: %d).",
                method,
                client_id.hex,
                workers,
                extra={
                    "sys": f"{SYSTEM_NAME}.reg-worker",
                }
            )
            self.methods[method].append(client_id)

        self.free_workers[client_id] += workers
        self.workers[client_id] += workers
        return {"ok": True}

    async def broker_handler(self):
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

        async with connection:
            channel = await connection.channel()
            rpc = await RPC.create(channel)
            self.client = rpc
            await rpc.register("reg_worker", self.reg_worker, auto_delete=True)
            await rpc.register("stop", self.stop_server, auto_delete=True)
            await self.wait_inner_call()

        self.logger.info("Broker processing spopping")

    async def wait_inner_call(self):
        """Polling of queue for next method call.
        """
        while self.state.get("active"):
            inner_method_call = await self.next_call_queue.get()
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
                self.state["stat"]["call_next"] += 1
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
        clients = self.workers.get(method)
        if clients:
            client = random.choice(clients)
            return client

        return ""

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
        await self.next_call_queue.put((method, new_params))
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

        while wait:
            out_client = None
            try_count += 1
            # select and call
            client = self.select_client(method, context)
            if client:
                # check free
                free = self.free_workers.get(client) or 0
                if free > 0:
                    self.free_workers[client] -= 1
                    try:
                        result = await self.client.call(
                            f"{method}__{client}",
                            kwargs=params,
                            priority=priority
                        )
                        if not isinstance(result, dict):
                            raise TypeError(
                                f"Incorrect result type {result.__class__}"
                            )

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
            self.broker_handler(),
            web._run_app(
                self.web_app,
                port=self.options.get("port"),
                host=self.options.get("host"),
                access_log=self.logger if debug else None,
                shutdown_timeout=self.shutdown_timeout
            )
        )
