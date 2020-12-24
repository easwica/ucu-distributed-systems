import flask
from flask import request, abort
import requests


app = flask.Flask(__name__)
app.config['DEBUG'] = True

msgs = {'messages': []}


@app.route('/api', methods=['GET'])
def homepage():
    return 'This is an API homepage.'


@app.route('/api/append', methods=['POST'])
def append():
    if not request.json:
        abort(400)
    msgs['messages'].append(request.json)

    ret_1 = requests.post('http://secondary_1_node:8081/api/append', json=request.json)
    ret_2 = requests.post('http://secondary_2_node:8082/api/append', json=request.json)

    if str(ret_1.status_code) != '200':
        abort(500)

    if str(ret_2.status_code) != '200':
        abort(500)
    
    return 'Successfully appended message!'


@app.route('/api/list_messages', methods=['GET'])
def list_messages():
    if not msgs['messages']:
        return 'No messages in the list!'
    return str(msgs)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
