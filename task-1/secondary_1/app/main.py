import flask
from flask import request, abort


app = flask.Flask(__name__)
app.config['DEBUG'] = True
msgs = {'messages': []}


@app.route('/api/append', methods=['POST'])
def append():
    if not request.json:
        abort(400)
    msgs['messages'].append(request.json)
    return 'Successfully processed message!'


@app.route('/api/list_messages', methods=['GET'])
def list_messages():
    if not msgs['messages']:
        return 'No messages!'
    return str(msgs)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081)
