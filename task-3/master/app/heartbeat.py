import asyncio

import aiohttp
from aiohttp import web
import json


async def periodic():
    while True:
        print('periodic')
        await asyncio.sleep(1)


async def send_message(ip_addr, msg):
    status_code, txt = 400, ''
    dumped = json.dumps(msg)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(ip_addr, json=dumped) as resp: 
                status_code = resp.status
                txt = await resp.text()
    except:
        pass

    return status_code, txt


async def send_messages2healthy_nodes():
    while True:
        try:
            ip_addr_1 = 'http://0.0.0.0:8081/api/check_health'
            async with aiohttp.ClientSession() as session: 
                async with session.get(ip_addr_1) as resp:
                    status_code_1 = resp.status
        except:
            status_code_1 = 400

        try:
            ip_addr_2 = 'http://0.0.0.0:8082/api/check_health'
            async with aiohttp.ClientSession() as session: 
                async with session.get(ip_addr_2) as resp:
                    status_code_2 = resp.status
        except:
            status_code_2 = 400

        if int(status_code_1) == 200:
            print('First node is healthy')
            with open('master/app/not_delivered_1.json', mode='rt') as fp:
                data = json.load(fp)
            
            if data:
                codes = []
                for message in data:
                    code, _ = await send_message('http://0.0.0.0:8081/api/append', message)
                    codes.append(code)
                
                remaining_not_delivered = [msg for ix, msg in enumerate(data) if int(codes[ix]) != 200]
                with open('master/app/not_delivered_1.json', mode='wt') as fp:
                    json.dump(remaining_not_delivered, fp)

        if int(status_code_2) == 200:
            print('Second node is healthy')
            with open('master/app/not_delivered_2.json', mode='rt') as fp:
                data = json.load(fp)
            
            if data:
                codes = []
                for message in data:
                    code, _ = await send_message('http://0.0.0.0:8082/api/append', message)
                    codes.append(code)
                
                remaining_not_delivered = [msg for ix, msg in enumerate(data) if int(codes[ix]) != 200]
                with open('master/app/not_delivered_2.json', mode='wt') as fp:
                    json.dump(remaining_not_delivered, fp)

        await asyncio.sleep(3)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = loop.create_task(send_messages2healthy_nodes())

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
