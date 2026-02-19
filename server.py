import asyncio
import websockets
import json
import os

connected_clients = set()

async def handler(websocket):
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            data = json.loads(message)
            username = data.get("username", "Unknown")
            text = data.get("message", "")

            broadcast = json.dumps({
                "username": username,
                "message": text
            })

            await asyncio.gather(*[
                client.send(broadcast)
                for client in connected_clients
                if client != websocket
            ])
    except:
        pass
    finally:
        connected_clients.remove(websocket)

async def main():
    port = int(os.environ.get("PORT", 8000))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Server running on port {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
