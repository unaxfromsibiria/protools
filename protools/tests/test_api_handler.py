import io
import json
import logging
import uuid
from unittest import mock

import pytest
from aiohttp.test_utils import make_mocked_request

from protools.logs import setup_logger
from protools.proserver import ApiHandler


def get_logger() -> (object, io.StringIO):
    buffer = io.StringIO()
    name = uuid.uuid4().hex
    handler = logging.StreamHandler(buffer)
    setup_logger(name=name, handler=handler)
    logger = logging.getLogger(name)
    return (logger, buffer)


@pytest.mark.asyncio
async def test_empty_requst():
    """Request without data.
    """
    logger, buffer = get_logger()
    state = {"active": True}

    async def call_intest(method, params, request):
        return {}

    api = ApiHandler(logger, state, call_intest)
    req = make_mocked_request("POST", "/api", )

    async def json_data(*args, **kwargs):
        return {}

    with mock.patch("aiohttp.web.Request.json", json_data):
        res = await api.http_handler(req)

    assert res is not None
    assert res.text
    data = json.loads(res.text)
    assert data.get("jsonrpc") == "2.0"
    error = data.get("error")
    assert error.get("code") == 400


@pytest.mark.asyncio
async def test_empty_with_hexid_requst():
    """Request without data and with uuid.
    """
    logger, buffer = get_logger()
    state = {"active": True}

    async def call_intest(method, params, request):
        return {"method": []}

    api = ApiHandler(logger, state, call_intest)
    api.id_as_int = False
    req = make_mocked_request("POST", "/api", )
    id_value = uuid.uuid4().hex

    async def json_data(*args, **kwargs):
        return {"id": id_value}

    with mock.patch("aiohttp.web.Request.json", json_data):
        res = await api.http_handler(req)

    assert res is not None
    assert res.text
    data = json.loads(res.text)
    assert data.get("jsonrpc") == "2.0"
    assert data.get("id") == id_value
    error = data.get("error")
    assert error.get("code") == 400
    assert "wrong name of method" in error["message"]


@pytest.mark.asyncio
async def test_empty_with_intid_requst():
    """Request without data and with uuid (as int).
    """
    logger, buffer = get_logger()
    state = {"active": True}

    async def call_intest(method, params, request):
        return {}

    api = ApiHandler(logger, state, call_intest)
    api.id_as_int = True
    req = make_mocked_request("POST", "/api", )
    id_value = int(uuid.uuid4().hex[:8], 16)

    async def json_data(*args, **kwargs):
        return {"id": id_value}

    with mock.patch("aiohttp.web.Request.json", json_data):
        res = await api.http_handler(req)

    assert res is not None
    assert res.text
    data = json.loads(res.text)
    assert data.get("jsonrpc") == "2.0"
    assert data.get("id") == id_value
    error = data.get("error")
    assert error.get("code") == 400


@pytest.mark.asyncio
async def test_empty_with_bad_id_requst():
    """Request without data and with wrong uuid.
    """
    logger, buffer = get_logger()
    state = {"active": True}

    async def call_intest(method, params, request):
        return {}

    api = ApiHandler(logger, state, call_intest)
    api.id_as_int = True
    req = make_mocked_request("POST", "/api", )
    id_value = "bad_uuid"

    async def json_data(*args, **kwargs):
        return {"id": id_value}

    with mock.patch("aiohttp.web.Request.json", json_data):
        res = await api.http_handler(req)

    assert res is not None
    assert res.text
    data = json.loads(res.text)
    assert data.get("jsonrpc") == "2.0"
    error = data.get("error")
    assert error.get("code") == 400
    log_line = buffer.getvalue()
    assert log_line
    log_event = json.loads(log_line)
    assert log_event
    assert log_event["loglevel"] == "ERROR"
    assert log_event["stacktrace"]
    assert "badly formed" in log_event["msg"]


@pytest.mark.asyncio
async def test_bad_method_params_requst():
    """Request without correct type of method params.
    """
    logger, buffer = get_logger()
    state = {"active": True}

    async def call_intest(method, params, request):
        return {}

    api = ApiHandler(logger, state, call_intest)
    api.id_as_int = True
    req = make_mocked_request("POST", "/api", )
    id_value = uuid.uuid4().int

    async def json_data(*args, **kwargs):
        return {"id": id_value, "method": "test", "params": [{"a": 1}]}

    with mock.patch("aiohttp.web.Request.json", json_data):
        res = await api.http_handler(req)

    assert res is not None
    assert res.text
    data = json.loads(res.text)
    assert data.get("jsonrpc") == "2.0"
    assert data.get("id") == id_value
    error = data.get("error")
    assert error.get("code") == 400
