from __future__ import annotations

import asyncio
import http
import logging
from typing import (
    Any,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)
from urllib.parse import unquote

import websockets
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.extensions.permessage_deflate import ServerPerMessageDeflateFactory
from websockets.legacy.server import HTTPResponse
from websockets.server import WebSocketServerProtocol
from websockets.typing import Subprotocol

from uvicorn._types import (
    ASGISendEvent,
    WebSocketAcceptEvent,
    WebSocketCloseEvent,
    WebSocketConnectEvent,
    WebSocketDisconnectEvent,
    WebSocketReceiveEvent,
    WebSocketResponseBodyEvent,
    WebSocketResponseStartEvent,
    WebSocketScope,
    WebSocketSendEvent,
)
from uvicorn.config import Config
from uvicorn.logging import TRACE_LOG_LEVEL
from uvicorn.protocols.utils import (
    Disconnected,
    get_local_addr,
    get_path_with_query_string,
    get_remote_addr,
    is_ssl,
)
from uvicorn.server import ServerState


class Server:
    closing = False

    def register(self, ws: WebSocketServerProtocol) -> None:
        pass

    def unregister(self, ws: WebSocketServerProtocol) -> None:
        pass

    def is_serving(self) -> bool:
        return not self.closing


class WebSocketProtocol(WebSocketServerProtocol):
    extra_headers: List[Tuple[str, str]]

    def __init__(
        self,
        config: Config,
        server_state: ServerState,
        app_state: dict[str, Any],
        _loop: asyncio.AbstractEventLoop | None = None,
    ):
        if not config.loaded:
            config.load()

        self.config = config
        self.app = config.loaded_app
        self.loop = _loop or asyncio.get_event_loop()
        self.root_path = config.root_path
        self.app_state = app_state

        # Shared server state
        self.connections = server_state.connections
        self.tasks = server_state.tasks

        # Connection state
        self.transport: asyncio.Transport = None  # type: ignore[assignment]
        self.server: tuple[str, int] | None = None
        self.client: tuple[str, int] | None = None
        self.scheme: Literal["wss", "ws"] = None  # type: ignore[assignment]

        # Connection events
        self.scope: WebSocketScope
        self.handshake_started_event = asyncio.Event()
        self.handshake_completed_event = asyncio.Event()
        self.closed_event = asyncio.Event()
        self.initial_response: HTTPResponse | None = None
        self.connect_sent = False
        self.lost_connection_before_handshake = False
        self.accepted_subprotocol: Subprotocol | None = None

        self.ws_server: Server = Server()  # type: ignore[assignment]

        extensions = []
        if self.config.ws_per_message_deflate:
            extensions.append(ServerPerMessageDeflateFactory())

        super().__init__(
            ws_handler=self.ws_handler,
            ws_server=self.ws_server,  # type: ignore[arg-type]
            max_size=self.config.ws_max_size,
            max_queue=self.config.ws_max_queue,
            ping_interval=self.config.ws_ping_interval,
            ping_timeout=self.config.ws_ping_timeout,
            extensions=extensions,
            logger=logging.getLogger("uvicorn.error"),
        )
        self.server_header = None
        self.extra_headers = [
            (name.decode("latin-1"), value.decode("latin-1"))
            for name, value in server_state.default_headers
        ]

    def connection_made(  # type: ignore[override]
        self, transport: asyncio.Transport
    ) -> None:
        self.connections.add(self)
        self.transport = transport
        self.server = get_local_addr(transport)
        self.client = get_remote_addr(transport)
        self.scheme = "wss" if is_ssl(transport) else "ws"

        if self.logger.isEnabledFor(TRACE_LOG_LEVEL):
            prefi
# ... [truncated] ...
tring(self.scope),
                    message["status"],
                )
                # websockets requires the status to be an enum. look it up.
                status = http.HTTPStatus(message["status"])
                headers = [
                    (name.decode("latin-1"), value.decode("latin-1"))
                    for name, value in message.get("headers", [])
                ]
                self.initial_response = (status, headers, b"")
                self.handshake_started_event.set()

            else:
                msg = (
                    "Expected ASGI message 'websocket.accept', 'websocket.close', "
                    "or 'websocket.http.response.start' but got '%s'."
                )
                raise RuntimeError(msg % message_type)

        elif not self.closed_event.is_set() and self.initial_response is None:
            await self.handshake_completed_event.wait()

            try:
                if message_type == "websocket.send":
                    message = cast("WebSocketSendEvent", message)
                    bytes_data = message.get("bytes")
                    text_data = message.get("text")
                    data = text_data if bytes_data is None else bytes_data
                    await self.send(data)  # type: ignore[arg-type]

                elif message_type == "websocket.close":
                    message = cast("WebSocketCloseEvent", message)
                    code = message.get("code", 1000)
                    reason = message.get("reason", "") or ""
                    await self.close(code, reason)
                    self.closed_event.set()

                else:
                    msg = (
                        "Expected ASGI message 'websocket.send' or 'websocket.close',"
                        " but got '%s'."
                    )
                    raise RuntimeError(msg % message_type)
            except ConnectionClosed as exc:
                raise Disconnected from exc

        elif self.initial_response is not None:
            if message_type == "websocket.http.response.body":
                message = cast("WebSocketResponseBodyEvent", message)
                body = self.initial_response[2] + message["body"]
                self.initial_response = self.initial_response[:2] + (body,)
                if not message.get("more_body", False):
                    self.closed_event.set()
            else:
                msg = (
                    "Expected ASGI message 'websocket.http.response.body' "
                    "but got '%s'."
                )
                raise RuntimeError(msg % message_type)

        else:
            msg = (
                "Unexpected ASGI message '%s', after sending 'websocket.close' "
                "or response already completed."
            )
            raise RuntimeError(msg % message_type)

    async def asgi_receive(
        self,
    ) -> Union[
        "WebSocketDisconnectEvent", "WebSocketConnectEvent", "WebSocketReceiveEvent"
    ]:
        if not self.connect_sent:
            self.connect_sent = True
            return {"type": "websocket.connect"}

        await self.handshake_completed_event.wait()

        if self.lost_connection_before_handshake:
            # If the handshake failed or the app closed before handshake completion,
            # use 1006 Abnormal Closure.
            return {"type": "websocket.disconnect", "code": 1006}

        if self.closed_event.is_set():
            return {"type": "websocket.disconnect", "code": 1005}

        try:
            data = await self.recv()
        except ConnectionClosed as exc:
            self.closed_event.set()
            if self.ws_server.closing:
                return {"type": "websocket.disconnect", "code": 1012}
            return {"type": "websocket.disconnect", "code": exc.code}

        if isinstance(data, str):
            return {"type": "websocket.receive", "text": data}
        return {"type": "websocket.receive", "bytes": data}

