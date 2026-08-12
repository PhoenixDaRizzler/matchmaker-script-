import asyncio
import json
import logging
import os
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OasisMatchmaker")

# In-memory registry of active hosts
# world_id -> {"websocket": ws, "public_ip": ip, "public_port": port}
active_hosts = {}

async def handle_client(websocket):
    # Depending on proxy setup, remote_address might be real IP, 
    # but on Render we should trust x-forwarded-for if present
    remote_ip = websocket.remote_address[0]
    remote_port = websocket.remote_address[1]
    
    # Handle websockets version differences
    if hasattr(websocket, 'request'):
        headers = websocket.request.headers
    else:
        headers = getattr(websocket, 'request_headers', {})
        
    if "x-forwarded-for" in headers:
        remote_ip = headers["x-forwarded-for"].split(",")[0].strip()

    logger.info(f"New connection from {remote_ip}:{remote_port}")
    current_world = None
    is_host = False

    try:
        async for message in websocket:
            data = json.loads(message)
            action = data.get("action")
            world_id = data.get("world_id")

            if not world_id:
                await websocket.send(json.dumps({"error": "Missing world_id"}))
                continue

            if action == "register_host":
                host_port = data.get("port", 25565)
                
                active_hosts[world_id] = {
                    "websocket": websocket,
                    "public_ip": remote_ip,
                    "public_port": host_port
                }
                current_world = world_id
                is_host = True
                logger.info(f"Host registered for world {world_id} at {remote_ip}:{host_port}")
                await websocket.send(json.dumps({"status": "registered", "public_ip": remote_ip}))

            elif action == "join":
                if world_id not in active_hosts:
                    await websocket.send(json.dumps({"error": "World not found or host offline"}))
                    continue

                host_info = active_hosts[world_id]
                client_port = data.get("port", 25565)

                logger.info(f"Client {remote_ip}:{client_port} is punching Host {host_info['public_ip']}:{host_info['public_port']}")

                # Tell Host to punch Client
                punch_host_msg = json.dumps({
                    "action": "punch",
                    "target_ip": remote_ip,
                    "target_port": client_port
                })
                
                # Tell Client to punch Host
                punch_client_msg = json.dumps({
                    "action": "punch",
                    "target_ip": host_info["public_ip"],
                    "target_port": host_info["public_port"]
                })

                await asyncio.gather(
                    host_info["websocket"].send(punch_host_msg),
                    websocket.send(punch_client_msg)
                )

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"Error handling client: {e}")
    finally:
        if is_host and current_world in active_hosts:
            if active_hosts[current_world]["websocket"] == websocket:
                del active_hosts[current_world]
                logger.info(f"Host for world {current_world} disconnected.")

async def main():
    port = int(os.environ.get("PORT", 8080))
    async with websockets.serve(handle_client, "0.0.0.0", port):
        logger.info(f"Matchmaker signaling server listening on port {port}")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
