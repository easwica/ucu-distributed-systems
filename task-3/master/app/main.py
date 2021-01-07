import asyncio
import json
import logging
import logging.handlers
import sys
import time

import aiohttp
from aiohttp import web

msgs = {'messages': []}


async def homepage(request):
    return web.Response(text='This is an API homepage.')


async def send_message(secondary_name, msg):
    async with aiohttp.ClientSession() as session:
        ip_addr = 'http://secondary_1_node:8081/api/append' if \
            secondary_name == 1 else 'http://secondary_2_node:8082/api/append'
        dumped = json.dumps(msg)
        async with session.post(ip_addr, json=dumped) as resp: 
            status_code = resp.status
            txt = await resp.text()
            return status_code, txt


async def check_health(request):
    try:
        async with aiohttp.ClientSession() as session: 
            ip_addr_1 = 'http://secondary_1_node:8081/api/check_health'
            async with session.get(ip_addr_1) as resp:
                status_code_1 = resp.status
            
            ip_addr_2 = 'http://secondary_2_node:8082/api/check_health'
            async with session.get(ip_addr_2) as resp:
                status_code_2 = resp.status

            if int(status_code_1) == int(status_code_2) and int(status_code_1) == 200:
                return web.Response(text='Healthy')
            else:
                raise ValueError('One of the nodes is unhealthy!')
    except:
        return web.Response(text='Unhealthy')


async def append_message(request):
    data = await request.json()

    if not data:
        sys.exit(1)

    msgs['messages'].append(data['message'])
    if int(data['concern']) == 1:
        return web.Response(text='Successfully appended message!')

    elif int(data['concern']) == 2:
        try:
            status_code, txt = await send_message(1, data)
            if int(status_code) == 200:
                return web.Response(text='Successfully appended message!')
            else:
                raise ValueError(txt)
        except:
            while True:
                try:
                    status_code_1, txt = await send_message(2, data)
                    if int(status_code_1) == 200:
                        break
                    time.sleep(3)
                    status_code_2, txt = await send_message(1, data)
                    if int(status_code_2) == 200:
                        break
                except:
                    continue
            return web.Response(text='Successfully appended message!')

    if int(data['concern']) == 3:
        try:
            status_code_1, txt_1 = await send_message(1, data)
            status_code_2, txt_2 = await send_message(2, data)
            if int(status_code_1) == 200 and int(status_code_2) == 200:
                return web.Response(text='Successfully appended messages!')
            raise ValueError(txt_1 + ', ' + txt_2)
        except:
            while True:
                try:
                    health = await check_health(None)
                    if health.text != 'Healthy':
                        time.sleep(3)
                    status_code_1, txt = await send_message(2, data)
                    time.sleep(3)
                    status_code_2, txt = await send_message(1, data)
                    if int(status_code_1) == 200 and int(status_code_2) == 200:
                        break
                except:
                    continue
            return web.Response(text='Successfully appended messages!')


async def list_messages(request):
    return web.json_response(msgs)


app = web.Application()
app.add_routes(
    [
        web.get('/api', homepage),
        web.get('/api/list_messages', list_messages),
        web.post('/api/append_message', append_message),
        web.get('/api/check_health', check_health)
    ]
)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8080)
