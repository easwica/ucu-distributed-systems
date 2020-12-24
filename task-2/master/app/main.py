import aiohttp
from aiohttp import web
import asyncio
import sys
import json
import logging
import logging.handlers


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


async def append_message(request):
    data = await request.json()

    if not data:
        sys.exit(1)

    msgs['messages'].append(data['message'])
    if int(data['concern']) == 1:
        return web.Response(text='Successfully appended message!')

    elif int(data['concern']) == 2:
        status_code, txt = await send_message(1, data)
        if str(status_code) == '200':
            return web.Response(text='Successfully appended message!')
        return web.Response(text=str(txt))

    if int(data['concern']) == 3:
        status_code_1, txt_1 = await send_message(1, data)
        status_code_2, txt_2 = await send_message(2, data)

        if str(status_code_1) == '200' and str(status_code_2) == '200':
            return web.Response(text='Successfully appended messages!')
        return web.Response(text=str(txt_1)) if \
            str(status_code_2) == '200' else web.Response(text=str(txt_2))


async def list_messages(request):
    return web.json_response(msgs)


app = web.Application()
app.add_routes(
    [
        web.get('/api', homepage),
        web.get('/api/list_messages', list_messages),
        web.post('/api/append_message', append_message)
    ]
)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8080)
