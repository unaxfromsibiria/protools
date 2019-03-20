import asyncio
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
        try:
            data = await request.json()
            request_id = data.get("id")
            if request_id and isinstance(request_id, int):
                event = UUID(f"{request_id:032x}")
            else:
                event = get_time_uuid()

            self.logger.set_event(event)

        except Exception as err:
            self.logger.error("Bad request: %s", err)
            status_code = web.HTTPServerError.status_code
            error = f"{err}"
        else:
            method_name = data.get("method")
            if method_name:
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
                error = "Incorrect JSON-RPC request (not exist method)"

            # TODO: JWT for params? (need token auth)

        # TODO: check method in registr
        # TODO: registr for methods and workers
        # TODO: rpc call
        # TODO: check result and call next method
        # TODO: big result and params save to redis (have to transfer without message broker)

        resp = {
            "id": event.int if self.id_as_int else event.hex,
            "jsonrpc": "2.0",
        }
        if answer:
            resp["result"] = answer
        else:
            resp["error"] = {"code": status_code, "message": error}

        return web.json_response(resp, status=status_code, dumps=ujson.dumps)


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

        self.state = {"active": True}
        self.logger = logger
        self.web_app = web.Application()
        self.workers = defaultdict(int)
        self.methods = defaultdict(list)
        self.free_workers = defaultdict(int)
        self.wait_free_timeout = env_var_float("METHOD_WAIT_TIME") or 5.0
        self.iter_delay = env_var_float("DEFAULT_ITER_DELAY") or 0.05

    def __str__(self) -> str:
        options = ",".join(
            f"{field}={value}"
            for field, value in self.options.items()
        )
        return f"{self.__class__.__name__}({options})"

    def __repr__(self) -> str:
        return self.__str__()

    async def manage(self):
        while self.state.get("active"):
            # TODO: run "next" methods for multistage processing
            await asyncio.sleep(self.iter_delay)

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

            while self.state.get("active"):
                # TODO: run "next" methods for multistage processing
                await asyncio.sleep(self.iter_delay)

        self.logger.info("Broker processing spopping")

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

    async def call_rpc(
            self, method: str, params: dict, request: web.Request) -> dict:
        """Access to client connection.
        """
        if self.debug:
            self.logger.debug("Call method '%s' with: %s", method, params)

        priority = int(params.get("priority") or self.default_priority)
        timeout = float(params.get("timeout") or self.wait_free_timeout)
        event = UUID(params["event"])

        wait = True
        exec_time = 0
        start_time = current_time()
        result = {}
        try_count = 0
        context = {}
        out_client = None

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
                            kwargs=result,
                            priority=priority
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
                    else:
                        wait = False
                    finally:
                        self.free_workers[client] += 1
            # #
            if wait:
                await asyncio.sleep(self.iter_delay)
                exec_time = current_time() - start_time

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
