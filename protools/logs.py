import json
import logging
import re
import sys
import uuid

from .helpers import current_time
from .helpers import env_var_bool
from .helpers import env_var_line
from .helpers import exception_wrapper
from .options import service_state

SYSTEM_NAME = "protools"
DEFAULT_LOGGER_NAME = "service"
DEFAULT_STDOUT_FORMATTER = (
    # valid json requeried
    # this way to use the format as:
    # log event as JSON -> stdout ->
    #   docker logging driver -> logstash -> elasticsearch
    # https://docs.docker.com/config/containers/logging/gelf/
    '{"@timestamp":"%(asctime)s.%(msecs)03d",'
    '"loglevel":"%(levelname)s",'
    '"log":"%(name)s","event":"%(event)s",'
    '"service":"%(service)032x",'
    '"line":"%(filename)s:%(lineno)d",'
    '"exec_time":%(exec_time)0.3f,'  # in ms
    '"count":%(count)d,'
    '"sys":%(system)s,'  # subsystem
    '"stacktrace":%(stacktrace)s,'
    '"msg":%(message)s}'
)


class SystemLogger(logging.getLoggerClass()):
    """Logger class with microservice configuration.
    """
    global_log_context = {}
    re_no_json = re.compile(r"[\"\{\}\n]+")
    extra_fields = None
    service_id = service_state["id"].int

    @classmethod
    def set_event(cls, current_event: uuid.UUID):
        cls.global_log_context["event"] = getattr(
            current_event, "hex", current_event
        )

    @classmethod
    def _convert_safe_msg(cls, msg: str, wrap: bool = True) -> str:
        """Message in safe format to use as value in JSON.
        """
        if msg:
            if isinstance(msg, Exception):
                msg = cls.re_no_json.sub("", str(msg))
                if msg:
                    msg = " ".join(re.split(r"[\s]+", msg))

            try:
                json_safe_msg = json.dumps(msg)
            except Exception as err:
                try:
                    json_safe_msg = "{}".format(str(msg))
                except Exception as errtostr:
                    json_safe_msg = f"message dump error: {err} and {errtostr}"
                if wrap:
                    json_safe_msg = '"{}"'.format(json_safe_msg)

            else:
                if not wrap:
                    json_safe_msg = json_safe_msg.strip('"')

        else:
            json_safe_msg = "null"

        return json_safe_msg

    # Method in camelCase required by parents class.
    def makeRecord(
            self, name, level, fn, lno, msg, args, exc_info,
            func=None, extra=None, sinfo=None):
        """Setup context to "extra".
        """
        if not isinstance(extra, dict):
            extra = {}
        else:
            start_time = extra.get("start_time")
            if start_time:
                # need to setup value of field "exec_time"
                try:
                    extra["exec_time"] = (
                        # in ms.
                        (current_time() - start_time) * 1000.0
                    )
                except:
                    pass
                finally:
                    del extra["start_time"]

        if "exec_time" not in extra:
            extra["exec_time"] = 0

        # event_id
        extra_event = extra.get("event")
        # event from extra
        if extra_event and isinstance(extra_event, uuid.UUID):
            extra_event = extra_event.hex
        elif extra_event:
            try:
                extra_event = uuid.UUID(extra_event).hex
            except (ValueError, AttributeError):
                raise ValueError(
                    "event ID value have to be in UUID format only."
                )
            finally:
                if "event_id" in extra:
                    del extra["event_id"]

        extra["event"] = (
            extra_event or self.__class__.global_log_context.get("event") or ""
        )

        extra["service"] = self.service_id
        if args:
            safe_args = tuple((
                (
                    self._convert_safe_msg(arg, False)
                    if isinstance(arg, (str, Exception)) else
                    arg
                )
                for arg in args
            ))
        else:
            safe_args = args

        # stacktrace
        stacktrace = None
        if isinstance(msg, Exception):
            stacktrace = exception_wrapper(msg, splitter=" ")
        elif args:
            for arg in args:
                if isinstance(arg, Exception):
                    stacktrace = exception_wrapper(arg, splitter=" ")
                    break

        if stacktrace:
            extra["stacktrace"] = " ".join(
                re.split(
                    r"[\s]+",
                    self._convert_safe_msg(self.re_no_json.sub("", stacktrace))
                )
            )
        else:
            extra["stacktrace"] = "null"

        # other fields
        for extra_field, def_val in self.extra_fields.items():
            if extra_field in extra:
                continue

            extra[extra_field] = def_val

        rec = super().makeRecord(
            name, level, fn, lno, self._convert_safe_msg(msg),
            safe_args, exc_info, func, extra, sinfo
        )
        return rec


def get_formatter_line() -> (str, Exception):
    """Open formatter line or use default.
    """
    file_open_err = None
    formatter = None
    path_formatter = env_var_line("LOG_STDOUT_FORMAT")
    if path_formatter:
        try:
            with open(path_formatter) as formatter_file:
                formatter = formatter_file.read()
        except Exception as err:
            file_open_err = err

    return (
        formatter or DEFAULT_STDOUT_FORMATTER,
        file_open_err
    )


def setup_logger(
        name: str = DEFAULT_LOGGER_NAME,
        handler: logging.StreamHandler = None):
    """Create logger.
    """
    logging.setLoggerClass(SystemLogger)
    debug = env_var_bool("DEBUG")
    if debug:
        level = env_var_line("LOGLEVEL").upper()
    else:
        level = "DEBUG"

    formatter, file_open_err = get_formatter_line()
    fields = re.findall(r"[(]\w+[)][\w.]+", formatter)
    extra_fields = {}
    std_fields = (
        "asctime", "msecs",
        "levelname", "name", "filename", "lineno", "message"
    )
    for field in fields:
        field_name, type_code = (item.strip("(") for item in field.split(")"))
        if field_name in std_fields:
            continue

        if type_code == "s":
            default_value = '""'
        elif type_code == "d":
            default_value = 0
        elif "f" in type_code:
            default_value = 0
        elif "x" in type_code:
            default_value = 0
        else:
            default_value = "null"

        extra_fields[field_name] = default_value

    SystemLogger.extra_fields = extra_fields
    formatter = logging.Formatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level, logging.INFO))
    if not handler:
        handler = logging.StreamHandler(sys.stdout)

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if file_open_err:
        logger.warning(
            "File with content for formatter: %s",
            file_open_err,
            extra={"sys": f"{SYSTEM_NAME}.init"}
        )


if env_var_bool("IMPORT_LOGGER_SETUP"):
    setup_logger()
