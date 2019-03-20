import asyncio
import logging

from dotenv import load_dotenv

from .helpers import env_var_bool
from .helpers import env_var_line
from .helpers import path_to_obj
from .logs import DEFAULT_LOGGER_NAME
from .logs import setup_logger
from .proserver import ProcessingServer

load_dotenv(env_var_line("ENV_FILE") or "service.env")

use_uvloop = not env_var_bool("WITHOUT_UVLOOP")
if use_uvloop:
    import uvloop
    uvloop.install()


server_cls = env_var_line("SERVER_CLASS")
if server_cls:
    # "protools.proserver.ProcessingServer" by default
    server_cls = path_to_obj(server_cls)
    assert issubclass(server_cls, ProcessingServer), "Incorrect server class"
else:
    server_cls = ProcessingServer

setup_logger()
logger = logging.getLogger(DEFAULT_LOGGER_NAME)
server = server_cls(logger)
logger.info(
    "Running processing server: %s uvloop: %s",
    server,
    "on" if use_uvloop else "off"
)
server.run()

loop = asyncio.get_event_loop()

try:
    loop.run_forever()
finally:
    loop.run_until_complete(server.broker_connection.close())
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
