#!/usr/bin/env python3
# vim:ts=4:sw=4:ai:et:si:sts=4


import asyncio
import cbor
from collections import OrderedDict
import time
import localstack_client.session as boto3
import re


toHex = lambda x: "0x%02X" % x
fromMilli = lambda x: x / 1000.0
fromCenti = lambda x: x / 100.0
hexRe = re.compile(r"(0x[0-9A-F]+)", re.I)

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

dynamodb = boto3.client("dynamodb", region_name="us-east-2")


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

        dynamodata = to_dynamodb_dict(data)
        # print("DynamoDB: %s" % dynamodata)

        response = dynamodb.put_item(TableName="webasto-readings", Item=dynamodata);
        httpcode = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500)
        if httpcode // 100 != 2:
            print("Response: %s" % response)

    print(f"Closing connection from {addr!r}")

    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_cbor_connection, "0.0.0.0", 8192)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print("Serving on %s" % addrs)

    async with server:
        await server.serve_forever()


def _to_dynamodb_item(value: any) -> dict:
    if value is True or value is False:
        return {"BOOL" : value}

    if value is None:
        return {"NULL" : true}

    if isinstance(value, str):
        if hexRe.match(value):
            value = int(value, 16)
        else:
            return {"S" : value}

    if isinstance(value, (int, float)):
        return {"N" : str(value)}

    if isinstance(value, bytes):
        return {"B" : value}

    if isinstance(value, dict):
        return {"M" : to_dynamodb_dict(value)}

    if isinstance(value, list):
        return {"L" : to_dynamodb_list(value)} 

    if isinstance(value, set):
        item = value.pop()
        value.add(item)

        if isinstance(item, bytes):
            return {"BS" : [v for v in value]}

        settype = "SS"
        if isinstance(item, (int, float)):
            settype = "NS"

        return {settype : [str(v) for v in value]}

    return {}


def to_dynamodb_dict(obj: dict) -> dict:
    return {key: _to_dynamodb_item(value) for (key, value) in obj.items()}


def to_dynamodb_list(lst: list) -> list:
    return [_to_dynamodb_item(value) for value in lst]


asyncio.run(main())

