from uuid import UUID
from uuid import uuid4

import jwt
from aiohttp import web

from .helpers import env_var_int
from .options import WorkerOptionEnum
from .proserver import ApiHandler
from .proserver import ProcessingServer


class JwtApiHandeler(ApiHandler):
    """JWT supported http handler for JSON RPC params.
    """

    client_fields_variants = (
        "client", "client_id", "user", "user_id", "uid"
    )

    service_src_token_rpc_method = "get_client_token"
    algorithms = ["HS256"]
    random_token = uuid4().hex

    async def pre_processing(
        self,
        event: UUID,
        method: str,
        method_options: dict,
        params: dict,
        request
    ) -> dict:
        """Extract params from token, with verification via service method.
        """
        result = {}
        if not method_options.get(WorkerOptionEnum.AUTH):
            return result

        token_data = params.get("data")
        try:
            data = jwt.decode(token_data, verify=False)
            assert isinstance(data, dict), (
                "Params format incorrect."
            )
        except Exception as err:
            result["error"] = f"JWT content error: {err}"
            result["status_code"] = web.HTTPBadRequest.status_code
        else:
            client_id = None
            for field in self.client_fields_variants:
                client_id = data.get(field)
                if client_id:
                    break

            if not client_id:
                fields = ", ".join(self.client_fields_variants)
                result["error"] = (
                    f"No fields for client identification, "
                    f"expected one of: {fields}"
                )
                result["status_code"] = web.HTTPBadRequest.status_code
            else:
                try:
                    user_token_value = await self.call(
                        self.service_src_token_rpc_method,
                        {"client": client_id},
                        request
                    )
                    assert isinstance(user_token_value, dict), (
                        "Method answer has incorrect format"
                    )
                except Exception as err:
                    result["error"] = f"JWT token souerce error: {err}"
                    result["status_code"] = web.HTTPServerError.status_code
                else:
                    # no empty token
                    token = user_token_value.get("token") or self.random_token
                    try:
                        data = jwt.decode(
                            token_data, token, algorithms=self.algorithms
                        )
                    except Exception as err:
                        result["error"] = f"Decoding error: {err}"
                        result["status_code"] = web.HTTPForbidden.status_code
                    else:
                        result["params"] = data

        return result


class JwtSupportedProcessingServer(ProcessingServer):
    """Processing server with JWT.
    """
    token_cache_timeout = 0
    token_cache_tpl = "proserver:user:{}:token"
    api_class = JwtApiHandeler

    def setup(self):
        """Read token cache options.
        """
        self.token_cache_timeout = env_var_int(
            "TOKEN_CACHE_TIMEOUT"
        )
        assert self.token_cache_timeout >= 0

    async def call_rpc(
        self,
        method: str,
        params: dict,
        request: web.Request = None
    ) -> dict:
        """Service method of token access.
        """
        result = cache_key = None
        token_access = method == self.api_class.service_src_token_rpc_method
        if token_access and self.token_cache_timeout:
            cache_key = self.token_cache_tpl.format(params.get("client"))
            result = await self.redis_pool.get(cache_key)
            if result:
                result = self._cache_value_manager.value_to_params(result)

        if result is None:
            result = await super().call_rpc(method, params)
            if token_access and cache_key:
                await self.redis_pool.set(
                    key=cache_key,
                    value=self._cache_value_manager.params_to_value(result),
                    expire=self.token_cache_timeout
                )

        return result
