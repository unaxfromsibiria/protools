import io
import json
import logging
import uuid
from unittest import mock

from protools.logs import DEFAULT_LOGGER_NAME
from protools.logs import setup_logger
from protools.helpers import current_time


def test_create_logger():
    """Simple logger create.
    """
    buffer = io.StringIO()
    name = uuid.uuid4().hex
    handler = logging.StreamHandler(buffer)
    setup_logger(name=name, handler=handler)
    logger = logging.getLogger(name)
    msg = f"test: {uuid.uuid4()}"
    logger.info(msg)
    line = buffer.getvalue()
    assert line
    row = json.loads(line)
    assert row
    assert row.get("msg") == msg


def test_exec_time():
    """Check value of exec time.
    """
    start_time = current_time()
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    setup_logger(handler=handler)
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    logger.error("Start time %f", start_time, extra={"start_time": start_time})
    line = buffer.getvalue()
    assert line
    row = json.loads(line)
    assert row
    exec_time = row.get("exec_time")
    assert exec_time
    test_time = (current_time() - start_time) * 1000.0
    assert test_time >= exec_time
    time_diff = test_time - exec_time
    assert 0 < time_diff <= 1


def test_stacktrace_format():
    """Check stacktrace format for errors.
    """
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    setup_logger(handler=handler)
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    info = "c = a / b error"
    try:
        a = 1
        b = "0"
        c = a / b
    except Exception as err:
        logger.critical(f"{info}: %s", err, extra={"count": a})
    else:
        assert not c

    line = buffer.getvalue()
    assert line
    row = json.loads(line)
    assert row and isinstance(row, dict)
    stacktrace = row.get("stacktrace")
    msg = row.get("msg")
    assert stacktrace
    assert msg
    assert "Traceback:" in stacktrace
    assert ".py:" in stacktrace
    assert info in msg
    assert "unsupported operand type" in msg


def test_stacktrace_valid_json():
    """Check stacktrace with smart message.
    """
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    setup_logger(handler=handler)
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)

    def create_error():
        raise ValueError("""
        Ood message
            {"msg": "text"}
            {\"bad\":\"value\"}
                {{'not': 'json'}}
        """)

    try:
        create_error()
    except Exception as err:
        logger.critical("Error: %s", err)
    else:
        assert not "no error?"

    line = buffer.getvalue()
    assert line
    row = json.loads(line)
    assert row and isinstance(row, dict)
    stacktrace = row.get("stacktrace")
    msg = row.get("msg")
    assert msg
    assert stacktrace
    parts = ["Ood message", "msg: text", "bad:value", "'not': 'json'"]
    for part in parts:
        assert part in stacktrace


def test_custom_format():
    """Check advanced field in log line.
    """

    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    test_format = (
        '{"@timestamp":"%(asctime)s.%(msecs)03d",'
        '"loglevel":"%(levelname)s",'
        '"log":"%(name)s","event":"%(event)s",'
        '"service":"%(service)032x",'
        '"line":"%(filename)s:%(lineno)d",'
        '"user":%(user)d,'
        '"exec_time":%(exec_time)0.3f,'  # in ms
        '"count":%(count)d,'
        '"sys":%(system)s,'  # subsystem
        '"stacktrace":%(stacktrace)s,'
        '"msg":%(message)s}'
    )

    def test_formatter():
        return (test_format, None)

    with mock.patch("protools.logs.get_formatter_line", test_formatter):
        setup_logger(handler=handler)

    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    user = int(uuid.uuid4().hex[:4], 16)
    logger.info("New line", extra={"user": user})

    line = buffer.getvalue()
    assert line
    row = json.loads(line)
    assert row and isinstance(row, dict)
    user_id = row.get("user")
    assert user == user_id
    # default 0
    buffer.truncate()
    buffer.seek(0)
    logger.info("No user", extra={"count": user})
    line = buffer.getvalue()
    assert line
    row = json.loads(line)
    assert row and isinstance(row, dict)
    user_id = row.get("user")
    assert user_id == 0
    assert row.get("count") == user
