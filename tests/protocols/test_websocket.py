import asyncio
import typing
from copy import deepcopy

import httpx
import pytest
import websockets
import websockets.client
import websockets.exceptions
from websockets.extensions.permessage_deflate import ClientPerMessageDeflateFactory
from websockets.typing import Subprotocol

from tests.response import Response
from tests.utils import run_server
from uvicorn._types import (
    ASGIReceiveCallable,
    ASGIReceiveEvent,
    ASGISendCallable,
    Scope,
    WebSocketCloseEvent,
    WebSocketDisconnectEvent,
    WebSocketResponseStartEvent,
)
from uvicorn.config import Config
from uvicorn.protocols.websockets.websockets_impl import WebSocketProtocol

try:
    from uvicorn.protocols.websockets.wsproto_impl import WSProtocol

    skip_if_no_wsproto = pytest.mark.skipif(False, reason="wsproto is installed.")
except ModuleNotFoundError:
    skip_if_no_wsproto = pytest.mark.skipif(True, reason="wsproto is not installed.")

if typing.TYPE_CHECKING:
    from uvicorn.protocols.http.h11_impl import H11Protocol
    from uvicorn.protocols.http.httptools_impl import HttpToolsProtocol


class WebSocketResponse:
    def __init__(
        self, scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
    ):
        self.scope = scope
        self.receive = receive
        self.send = send

    def __await__(self):
        return self.asgi().__await__()

    async def asgi(self):
        while True:
            message = await self.receive()
            message_type = message["type"].replace(".", "_")
            handler = getattr(self, message_type, None)
            if handler is not None:
                await handler(message)
            if message_type == "websocket_disconnect":
                break


async def wsresponse(url):
    """
    A simple websocket connection request and response helper
    """
    url = url.replace("ws:", "http:")
    headers = {
        "connection": "upgrade",
        "upgrade": "websocket",
        "Sec-WebSocket-Key": "x3JJHMbDL1EzLkh9GBhXDw==",
        "Sec-WebSocket-Version": "13",
    }
    async with httpx.AsyncClient() as client:
        return await client.get(url, headers=headers)


@pytest.mark.anyio
async def test_invalid_upgrade(
    ws_protocol_cls: "typing.Type[WSProtocol | WebSocketProtocol]",
    http_protocol_cls: "typing.Type[H11Protocol | HttpToolsProtocol]",
    unused_tcp_port: int,
):
    def app(scope: Scope):
        return None

    config = Config(
        app=app, ws=ws_protocol_cls, http=http_protocol_cls, port=unused_tcp_port
    )
    async with run_server(config):
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"http://127.0.0.1:{unused_tcp_port}",
                headers={
                    "upgrade": "websocket",
                    "connection": "upgrade",
                    "sec-webSocket-version": "11",
                },
            )
        if response.status_code == 426:
            # response.text == ""
            pass  # ok, wsproto 0.13
        else:
            assert response.status_code == 400
            assert (
                response.text.lower().strip().rstrip(".")
                in [
                    "missing sec-websocket-key header",
                    "missing sec-websocket-version header",  # websockets
                    "missing or empty sec-websocket-key header",  # wsproto
                    "failed to open a websocket connection: missing "
                    "sec-websocket-key header",
                    "failed to open a websocket connection: missing or empty "
                    "sec-websocket-key header",
                ]
            )


@pytest.mark.anyio
async def test_accept_connection(
    ws_protocol_cls: "typing.Type[WSProtocol | WebSocketProtocol]",
    http_protocol_cls: "typing.Type[H11Protocol | HttpToolsProtocol]",
    unused_tcp_port: int,
):
    class App(WebSocketResponse):
        async def websocket_connect(self, message):
            await 
# ... [truncated] ...
ers = await open_connection(f"ws://127.0.0.1:{unused_tcp_port}")
        assert "server" not in headers


@pytest.mark.anyio
@skip_if_no_wsproto
async def test_no_date_header_on_wsproto(
    http_protocol_cls: "typing.Type[H11Protocol | HttpToolsProtocol]",
    unused_tcp_port: int,
):
    class App(WebSocketResponse):
        async def websocket_connect(self, message):
            await self.send({"type": "websocket.accept"})

    async def open_connection(url: str):
        async with websockets.client.connect(url) as websocket:
            return websocket.response_headers

    config = Config(
        app=App,
        ws=WSProtocol,
        http=http_protocol_cls,
        lifespan="off",
        date_header=False,
        port=unused_tcp_port,
    )
    async with run_server(config):
        headers = await open_connection(f"ws://127.0.0.1:{unused_tcp_port}")
        assert "date" not in headers


@pytest.mark.anyio
async def test_multiple_server_header(
    ws_protocol_cls: "typing.Type[WSProtocol | WebSocketProtocol]",
    http_protocol_cls: "typing.Type[H11Protocol | HttpToolsProtocol]",
    unused_tcp_port: int,
):
    class App(WebSocketResponse):
        async def websocket_connect(self, message):
            await self.send(
                {
                    "type": "websocket.accept",
                    "headers": [
                        (b"Server", b"over-ridden"),
                        (b"Server", b"another-value"),
                    ],
                }
            )

    async def open_connection(url: str):
        async with websockets.client.connect(url) as websocket:
            return websocket.response_headers

    config = Config(
        app=App,
        ws=ws_protocol_cls,
        http=http_protocol_cls,
        lifespan="off",
        port=unused_tcp_port,
    )
    async with run_server(config):
        headers = await open_connection(f"ws://127.0.0.1:{unused_tcp_port}")
        assert headers.get_all("Server") == ["uvicorn", "over-ridden", "another-value"]


@pytest.mark.anyio
async def test_lifespan_state(
    ws_protocol_cls: "typing.Type[WSProtocol | WebSocketProtocol]",
    http_protocol_cls: "typing.Type[H11Protocol | HttpToolsProtocol]",
    unused_tcp_port: int,
):
    expected_states = [
        {"a": 123, "b": [1]},
        {"a": 123, "b": [1, 2]},
    ]

    actual_states = []

    async def lifespan_app(
        scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
    ):
        message = await receive()
        assert message["type"] == "lifespan.startup" and "state" in scope
        scope["state"]["a"] = 123
        scope["state"]["b"] = [1]
        await send({"type": "lifespan.startup.complete"})
        message = await receive()
        assert message["type"] == "lifespan.shutdown"
        await send({"type": "lifespan.shutdown.complete"})

    class App(WebSocketResponse):
        async def websocket_connect(self, message):
            actual_states.append(deepcopy(self.scope["state"]))
            self.scope["state"]["a"] = 456
            self.scope["state"]["b"].append(2)
            await self.send({"type": "websocket.accept"})

    async def open_connection(url: str):
        async with websockets.client.connect(url) as websocket:
            return websocket.open

    async def app_wrapper(
        scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
    ):
        if scope["type"] == "lifespan":
            return await lifespan_app(scope, receive, send)
        return await App(scope, receive, send)

    config = Config(
        app=app_wrapper,
        ws=ws_protocol_cls,
        http=http_protocol_cls,
        lifespan="on",
        port=unused_tcp_port,
    )
    async with run_server(config):
        is_open = await open_connection(f"ws://127.0.0.1:{unused_tcp_port}")
        assert is_open
        is_open = await open_connection(f"ws://127.0.0.1:{unused_tcp_port}")
        assert is_open

    assert expected_states == actual_states

