import json
import random

import flask
import numpy as np
from flask import request

app = flask.Flask(__name__)
app.config['DEBUG'] = True

with open('secondary_2/app/storage.json', mode='rt') as fp:
    msgs = json.load(fp)
ids = [int(msg['id']) for msg in msgs['messages']]


def generate_error():
    rand_num = random.randint(0, 20)
    if rand_num % 3 == 0:
        raise ValueError('This is just a random error to be raised')


def write2storage():
    with open('secondary_2/app/storage.json', mode='wt') as fp:
        json.dump(msgs, fp)


@app.route('/api/append', methods=['POST'])
def append():
    try:
        data = json.loads(request.json)
        if int(data['id']) not in ids:
            msgs['messages'].append(data)
            ids.append(int(data['id']))
            write2storage()

        generate_error()
        return 'Successfully processed message!', 200
    except:
        return 'Not able to process message!', 400


def get_ordered_slice():
    ixs = np.argsort(ids)

    sliced = []
    for cnt, ix in enumerate(ixs):
        if int(msgs['messages'][ix]['id']) == cnt + 1:
            sliced.append(msgs['messages'][ix])
        else:
            break
    return sliced


@app.route('/api/list_messages', methods=['GET'])
def list_messages():
    if not msgs['messages']:
        return 'No messages!'

    sliced = get_ordered_slice()
    return str(sliced)


@app.route('/api/check_health', methods=['GET'])
def check_health():
    return 'Healthy'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082)
