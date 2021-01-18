import asyncio
import json
import logging
import logging.handlers
import sys
import time
import threading
import numpy as np
import random

import aiohttp
from aiohttp import web

msgs, ids = {'messages': []}, []


async def homepage(request):
    return web.Response(text='This is an API homepage.')


def append_not_delivered2storage(msg, status_code_1, status_code_2):
    if status_code_1 != 200:
        with open('master/app/not_delivered_1.json', mode='rt') as fp:
            data = json.load(fp)
            ids_1 = [ms['id'] for ms in data]
            if msg['id'] not in ids_1:
                data.append(msg)
        
        with open('master/app/not_delivered_1.json', mode='wt') as fp:
            json.dump(data, fp)
    
    if status_code_2 != 200:
        with open('master/app/not_delivered_2.json', mode='rt') as fp:
            data = json.load(fp)
            ids_2 = [ms['id'] for ms in data]
            if msg['id'] not in ids_2:
                data.append(msg)
        
        with open('master/app/not_delivered_2.json', mode='wt') as fp:
            json.dump(data, fp)


async def send_message(concern, msg, latch):
    ip_addr_1, ip_addr_2 = 'http://0.0.0.0:8081/api/append', 'http://0.0.0.0:8082/api/append'
    status_code_1, status_code_2, txt_1, txt_2 = 400, 400, '', ''
    dumped = json.dumps(msg)
    if concern == 1:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(ip_addr_1, json=dumped) as resp: 
                    status_code_1 = resp.status
                    txt_1 = await resp.text()
                    if int(status_code_1) == 200:
                        latch.count_down()
            async with aiohttp.ClientSession() as session:
                async with session.post(ip_addr_2, json=dumped) as resp: 
                    status_code_2 = resp.status
                    txt_2 = await resp.text()
                    if int(status_code_1) == 200:
                        latch.count_down()
            append_not_delivered2storage(msg, int(status_code_1), int(status_code_2))
        except:
            append_not_delivered2storage(msg, int(status_code_1), int(status_code_2))
    else:
        if concern == 2:
            while int(status_code_1) != 200 and int(status_code_2) != 200:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(ip_addr_1, json=dumped) as resp: 
                            status_code_1 = resp.status
                            txt_1 = await resp.text()
                            if int(status_code_1) == 200:
                                latch.count_down()
                    async with aiohttp.ClientSession() as session:
                        async with session.post(ip_addr_2, json=dumped) as resp: 
                            status_code_2 = resp.status
                            txt_2 = await resp.text()
                            if int(status_code_2) == 200:
                                latch.count_down()
                    append_not_delivered2storage(msg, int(status_code_1), int(status_code_2))
                except:
                    append_not_delivered2storage(msg, int(status_code_1), int(status_code_2))
        else:
            while int(status_code_1) + int(status_code_2) != 400:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(ip_addr_1, json=dumped) as resp: 
                            status_code_1 = resp.status
                            txt_1 = await resp.text()
                            if int(status_code_1) == 200:
                                latch.count_down()
                    async with aiohttp.ClientSession() as session:
                        async with session.post(ip_addr_2, json=dumped) as resp: 
                            status_code_2 = resp.status
                            txt_2 = await resp.text()
                            if int(status_code_1) == 200:
                                latch.count_down()
                    append_not_delivered2storage(msg, int(status_code_1), int(status_code_2))
                except:
                    append_not_delivered2storage(msg, int(status_code_1), int(status_code_2))
    return status_code_1, status_code_2, txt_1, txt_2


def between_callback(concern, msg, latch):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(send_message(concern, msg, latch))
    loop.close()


async def check_health(request):
    try:
        async with aiohttp.ClientSession() as session: 
            ip_addr_1 = 'http://0.0.0.0:8081/api/check_health'
            async with session.get(ip_addr_1) as resp:
                status_code_1 = resp.status
    except:
        status_code_1 = 400
    
    try:
        ip_addr_2 = 'http://0.0.0.0:8082/api/check_health'
        async with session.get(ip_addr_2) as resp:
            status_code_2 = resp.status
    except:
        status_code_2 = 400

    if int(status_code_1) == int(status_code_2) and int(status_code_1) == 200:
        return web.Response(text='Both nodes are healthy')
    else:
        if int(status_code_1) == 200 and int(status_code_2) == 400:
            return web.Response(text='The second node is unhealthy!')
        elif int(status_code_2) == 200 and int(status_code_1) == 400:
            return web.Response(text='The first node is unhealthy!')
        else:
            return web.Response(text='Both nodes are unhealthy')


class CountDownLatch:
    def __init__(self, count=1):
        self.count = count
        self.lock = threading.Condition()

    def count_down(self):
        try:
            self.lock.acquire()
            self.count -= 1
            if self.count <= 0:
                self.lock.notifyAll()
        finally:
            self.lock.release()

    def wait(self):
        try:
            self.lock.acquire()
            while self.count > 0:
                self.lock.wait()
        finally:
            self.lock.release()


def run(latch):
    print('Lock is waiting...')
    latch.wait()
    print('Lock is running and being released')


async def append_message(request):
    data = await request.json()
    if int(data['id']) not in ids:
        msgs['messages'].append(data)
        ids.append(int(data['id']))

    latch = CountDownLatch(int(data['concern']) - 1)
    t1 = threading.Thread(target=run, args=(latch,))
    t1.start()

    print('Message has a concern: ', data['concern'])
    t2 = threading.Thread(target=between_callback, args=(int(data['concern']), data, latch, ))

    t2.start()
    t2.join()
    return web.Response(text='Successfully appended message!')


def get_ordered_slice():
    ixs = np.argsort(ids)

    sliced = []
    for cnt, ix in enumerate(ixs):
        if int(msgs['messages'][ix]['id']) == cnt + 1:
            sliced.append(msgs['messages'][ix])
        else:
            break
    return sliced


async def list_messages(request):
    if not msgs['messages']:
        return 'No messages!'

    sliced = get_ordered_slice()
    return web.Response(text=str(sliced))


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
