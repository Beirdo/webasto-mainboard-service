#!/usr/bin/env python3
# vim:ts=4:sw=4:ai:et:si:sts=4


import asyncio
import cbor
from collections import OrderedDict
import time


toHex = lambda x: "0x%02X" % x
fromMilli = lambda x: x / 1000.0
fromCenti = lambda x: x / 100.0


keyMap = OrderedDict({
    "version" : int, 
    "packet_type": int, 
    "buffer" : bytes, 
    "fsm_state" : toHex, 
    "fsm_mode" : toHex, 
    "burn_power" : int,
    "flame_detect" : fromMilli,
    "combustion_fan" : int,
    "vehicle_fan" : int,
    "internal_temp" : fromCenti,
    "outdoor_temp" : fromCenti,
    "coolant_temp" : fromCenti,
    "exhaust_temp" : fromCenti,
    "battery_voltage" : fromMilli,
    "vsys_voltage" : fromMilli,
    "gpios": toHex,
})

keyNameMap = {index: name for (index, name) in enumerate(keyMap.keys())}
formatMap = {index: func for (index, func) in enumerate(keyMap.values())}


async def handle_cbor_connection(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Connection from {addr!r}")

    while True:
        rawdata = await reader.read(200)
        if not rawdata:
            break

        try:
            data = cbor.loads(rawdata)
        except Exception as e:
            print("Exception: %s" % e)
            continue

        data = {keyNameMap[key]: formatMap[key](value) for (key, value) in data.items() if key in keyNameMap and key in formatMap}
        data["timestamp"] = time.time()
        print("Message: %s" % data)

    print(f"Closing connection from {addr!r}")

    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_cbor_connection, "0.0.0.0", 8192)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print("Serving on %s" % addrs)

    async with server:
        await server.serve_forever()


asyncio.run(main())

