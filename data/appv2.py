import asyncio
import websockets
import json
import re
import pandas as pd
from datetime import datetime, timezone
import os
import ssl
import comlibv3
import logging
from flask import Flask, jsonify

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_KEY = "ble-xhnm-s6rn-m9zy-7ic6-6eu7-gh1r-lswm-zgi8"
#ENDPOINT = "wss://hqndvjem001.ds.centcom.mil/uhu/omn1"
#SERVER_URI = f"{ENDPOINT}?apikey={API_KEY}"
SERVER_URI = f"wss://172.16.0.46:8443/ble/lazy.goby?apikey={API_KEY}"
message_queue = asyncio.Queue()

INITIAL_RECONNECT_DELAY = 3    # seconds
MAX_RECONNECT_DELAY     = 30   # seconds — cap so we don't hammer the server on sustained outages


async def connect_stream():
    """Connects to the WSS endpoint and forwards messages into message_queue.
    Reconnects with backoff on any disconnect instead of exiting the process —
    recovery previously depended entirely on Docker restarting the container."""
    delay = INITIAL_RECONNECT_DELAY
    while True:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        try:
            async with websockets.connect(SERVER_URI, ssl=ssl_context) as websocket:
                logging.info(f"[Client] Connected to {SERVER_URI}")
                delay = INITIAL_RECONNECT_DELAY  # reset backoff after a successful connection
                while True:
                    raw_message = await websocket.recv()
                    #logging.info(f"[Raw] {raw_message}")
                    message_dict = json.loads(raw_message)
                    #logging.info(message_dict)
                    await message_queue.put(message_dict)
        except websockets.exceptions.ConnectionClosedOK:
            logging.warning("[Client] Connection closed by server")
        except Exception as e:
            logging.error(f"[Client] Error: {e}")

        logging.info(f"[Client] Reconnecting in {delay}s...")
        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RECONNECT_DELAY)

async def process_messages():
    while True:
        message_json = await message_queue.get()
        cef_headers = "CEF:0|CENTCOM|J6-DTT|{}|{}|BLE-VIZ|DEV|" 
        
        if message_json:
            cef_messages = comlibv3.data_to_cef(message_json, cef_headers)
            logging.info(f"Received {len(message_json)} messages")
            file_json = json.dumps(message_json, indent=2)
            with open("output.json", "w") as f:
                f.write(file_json)
        else:
            logging.error(f"Expected dict, got {type(message_json)}")

        #logging.info(f"[CEF] Converted: {(cef_messages)}")
        with open('./blecef.log', 'w', encoding='utf-8') as file:
            file.write(cef_messages)

        proxy_addr = '172.16.0.46'
        proxy_port = 9001
        #comlibv3.send_data_over_tcp(cef_messages, proxy_addr, proxy_port)
        comlibv3.send_events_over_udp(cef_messages, proxy_addr, proxy_port)
        logging.info(f"Sent {len(message_json)} CEF messages.")


async def main():
    # process_messages() just drains message_queue forever — it doesn't need
    # to restart on reconnect, only connect_stream()'s receive loop does.
    asyncio.create_task(process_messages())
    await connect_stream()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("Shutting down process gracefully")
        exit(0)

