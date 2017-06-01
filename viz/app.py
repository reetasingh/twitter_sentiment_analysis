from flask import Flask, render_template, Response

import redis

app = Flask(__name__)
r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)


def event_stream_wordcount():
    pubsub = r.pubsub()
    pubsub.subscribe('companywordcloud')
    for message in pubsub.listen():
        print message
        yield 'data: %s\n\n' % message['data']

def event_stream_stockvalue():
    pubsub = r.pubsub()
    pubsub.subscribe('stockgraph')
    for message in pubsub.listen():
        print message
        yield 'data: %s\n\n' % message['data']


@app.route('/')
def show_homepage():
  #Word Cloud = cloud.html and app-cloud.js
    return render_template("cloud.html")

@app.route('/basic')
def show_basic():
  #Basic d3 view = basic.html and app.js
    return render_template("basic.html")

@app.route('/streamcompanywordcount')
def stream_wordcount():
    return Response(event_stream_wordcount(), mimetype="text/event-stream")

@app.route('/streamcompanystockvalue')
def stream_stockvalue():
    return Response(event_stream_stockvalue(), mimetype="text/event-stream")   

if __name__ == '__main__':
    app.run(threaded=True,
    host='0.0.0.0'
)
