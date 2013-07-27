#!/usr/bin/env python
import sys
import flask
import json

app = flask.Flask(__name__)


@app.route('/')
def main():
    data = open('test_data.txt','r').read()
    data = json.loads(data)
    return data['data']

if __name__ == '__main__':
    app.debug = True
    app.run()
