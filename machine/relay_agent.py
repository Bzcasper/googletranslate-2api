"""Network-only bridge: authenticated Modal WebSockets -> existing bobby proxy.

No translation or key extraction runs on bobby. Proxy credentials stay on bobby.
"""
import asyncio
import base64
import json
import logging
import sys
from pathlib import Path

import websockets

logging.getLogger("websockets").setLevel(logging.CRITICAL)


def credentials(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip("\"'")
    raw = (values["PROXY_USER"] + ":" + values["PROXY_PASS"]).encode()
    return base64.b64encode(raw)


def permitted(authority):
    host, _, port = authority.rpartition(":")
    return port == "443" and (
        host in {"api.ipify.org", "translate.google.com", "translate-pa.googleapis.com"}
        or host.endswith((".google.com", ".googleapis.com", ".gstatic.com"))
    )


async def stream(config, stream_id):
    headers = {"Authorization": "Bearer " + config["token"]}
    proxy_writer = None
    try:
        async with websockets.connect(
            config["url"] + "/relay/stream/" + stream_id,
            additional_headers=headers, max_size=2**20, ping_interval=20,
        ) as ws:
            initial = bytearray()
            while b"\r\n\r\n" not in initial:
                initial.extend(await asyncio.wait_for(ws.recv(), 30))
                if len(initial) > 16384:
                    raise ValueError("header too large")
            first, rest = bytes(initial).split(b"\r\n", 1)
            parts = first.decode("ascii").split()
            if len(parts) != 3 or parts[0] != "CONNECT" or not permitted(parts[1]):
                await ws.send(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                return
            proxy_reader, proxy_writer = await asyncio.open_connection("127.0.0.1", 7894)
            auth = credentials(config["proxy_credentials"])
            proxy_writer.write(first + b"\r\nProxy-Authorization: Basic " + auth + b"\r\n" + rest)
            await proxy_writer.drain()

            async def to_proxy():
                async for data in ws:
                    if not isinstance(data, bytes):
                        raise ValueError("expected binary transport")
                    proxy_writer.write(data)
                    await proxy_writer.drain()

            async def to_modal():
                while data := await proxy_reader.read(65536):
                    await ws.send(data)

            tasks = [asyncio.create_task(to_proxy()), asyncio.create_task(to_modal())]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception:
        # Never include request bytes or credentials in a network error log.
        pass
    finally:
        if proxy_writer is not None:
            proxy_writer.close()
            await proxy_writer.wait_closed()


async def main(config):
    tasks = set()
    while True:
        try:
            async with websockets.connect(
                config["url"] + "/relay/control",
                additional_headers={"Authorization": "Bearer " + config["token"]},
                ping_interval=20, max_size=16384,
            ) as ws:
                print("Modal proxy relay connected", flush=True)
                async for raw in ws:
                    message = json.loads(raw)
                    if message.get("op") == "open":
                        task = asyncio.create_task(stream(config, message["id"]))
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)
        except Exception:
            print("Modal proxy relay reconnecting", flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main(json.loads(Path(sys.argv[1]).read_text())))
