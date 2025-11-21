# bridge.py
import asyncio
import websockets

WEBSOCKET_URL = "ws://localhost:8000/websocket"
TCP_IP = "127.0.0.1"
TCP_PORT = 9999

clients = []

async def handle_tcp_client(reader, writer):
    print(" Spark connected!")
    clients.append(writer)
    try:
        while True:
            await asyncio.sleep(1)
    except Exception as e:
        print(" TCP client error:", e)
    finally:
        writer.close()
        await writer.wait_closed()
        clients.remove(writer)

async def tcp_server():
    server = await asyncio.start_server(handle_tcp_client, TCP_IP, TCP_PORT)
    print(f" TCP server listening on {TCP_IP}:{TCP_PORT}")
    async with server:
        await server.serve_forever()

# WebSocket forwarder
async def forward_ws():
    async with websockets.connect(WEBSOCKET_URL) as ws:
        print(f"Connected to WebSocket {WEBSOCKET_URL}")
        while True:
            msg = await ws.recv()
            for writer in clients:
                writer.write((msg + "\n").encode("utf-8"))
                await writer.drain()
            print("Sent to Spark:", msg)

async def main():
    await asyncio.gather(tcp_server(), forward_ws())

if __name__ == "__main__":
    asyncio.run(main())
