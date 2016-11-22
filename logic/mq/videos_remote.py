import os
import sys
import json
import tornado.web
import tornado.escape
# import logic.dtm as dtm

# print sys.modules
if 'logic.dtm' not in sys.modules:
    import logic.dtm as dtm
else:
    dtm = sys.modules['logic.dtm']

timeout = 5

REQUEST = 'REQUEST'
REQUEST_ANSWER = 'REQUEST_ANSWER'
EXCHANGE = 'videos.test'
ROUTING_KEY = 'videos.general.app2'
GENERAL_KEY = 'videos.general'

results = {}

@tornado.gen.coroutine
def on_message(mq, _id, _from, status, to, message):
    if _from != 'app2':
        if status == REQUEST:
            print REQUEST
            local_videos = yield dtm.get_local_videos()
            print local_videos
            mq.send_message(local_videos, EXCHANGE, ROUTING_KEY, to, REQUEST_ANSWER, _id)
        elif status == REQUEST_ANSWER:
            if _id in results:
                print message
                results[_id].append(message)

@tornado.gen.coroutine
def get_videos(mq):
    _id = mq.send_message('', EXCHANGE, ROUTING_KEY, GENERAL_KEY, REQUEST)
    results[_id] = []
    yield tornado.gen.sleep(timeout)
    response = results.pop(_id)
    response = [x['videos'] for x in response]
    response = reduce(lambda x, y: x+y, response, [])
    print response
    raise tornado.gen.Return(response)

    