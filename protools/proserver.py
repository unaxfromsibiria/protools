import asyncio
from collections import defaultdict
from uuid import UUID

from aio_pika import connect_robust
from aio_pika.patterns import RPC
from aiohttp import web

from .helpers import env_var_bool
from .helpers import env_var_int
from .helpers import env_var_line
from .helpers import get_time_uuid


class ApiHandler:
    """Json-rpc 2.0 adapter.
    """
    logger = None
    version = "2.0"
    id_as_int = True

    def __init__(self, logger: object):
        """
        """
        # TODO: create brocker connection
        self.logger = logger

    async def http_handler(self, request, *args, **kwargs):
        """Parse request as json rpc request.
        """
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
            raise web.HTTPServerError(text=f"Error: {err}")
        else:
            method_name = data.get("method")
            if not method_name:
                raise web.HTTPBadRequest(
                    text="Incorrect JSON-RPC request (not exist method)"
                )

            params = data.get("params") or {}
            if not isinstance(params, dict):
                raise web.HTTPBadRequest(
                    text="Unsupported type of 'params'"
                )
            # TODO: JWT for params? (need token auth)

        # TODO: check method in registr
        # TODO: registr for methods and workers
        # TODO: rpc call
        # TODO: check result and call next method
        # TODO: big result and params save to redis (have to transfer without message broker)

        return web.json_response(
            {
                "id": event.int if self.id_as_int else event.hex,
                "jsonrpc": "2.0",
                "result": data
            }
        )


class ProcessingServer:
    """Processing server.
    """
    options = logger = None
    web_app = None
    api_class = ApiHandler
    shutdown_timeout = 0.5

    def __init__(self, logger: object):
        self.options = {
            "port": env_var_int("HTTP_PORT") or None,
            "host": env_var_line("HTTP_HOST") or None,
        }
        self.logger = logger
        self.web_app = web.Application()

    def __str__(self) -> str:
        options = ",".join(
            f"{field}={value}"
            for field, value in self.options.items()
        )
        return f"{self.__class__.__name__}({options})"

    def __repr__(self) -> str:
        return self.__str__()

    async def manage(self):
        while True:
            # TODO: run "next" methods for multistage processing
            await asyncio.sleep(1)

    def advanced_handlers(self) -> list:
        """Other handlers.
        """
        return []

    def run(self):
        debug = env_var_bool("DEBUG")
        host = self.options.get("host")
        port = self.options.get("port")
        api = self.api_class(self.logger)

        self.web_app.add_routes(
            [
                web.post('/api', api.http_handler)
            ] + (
                self.advanced_handlers()
            )
        )

        asyncio.gather(
            self.manage(),
            # TODO: rpc server with methods:
            #   - add_client
            #   - down_processing
            #   - get_state
            web._run_app(
                self.web_app,
                port=port,
                host=host,
                access_log=self.logger if debug else None,
                shutdown_timeout=self.shutdown_timeout
            )
        )
