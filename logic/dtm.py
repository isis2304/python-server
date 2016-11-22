import os
import tm
import sys
import json
import datetime
import tornado.web
import tornado.escape
import mq.videos_remote as videos_remote


@tornado.gen.coroutine
def request_videos_remote(mq):
    results = yield videos_remote.get_videos(mq)
    raise tornado.gen.Return(results)

@tornado.gen.coroutine
def get_local_videos():
    results = yield tm.get_videos_local()
    raise tornado.gen.Return(results)    