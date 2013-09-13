#!/usr/bin/env python
from celery import Celery
from crawl_tools import crawl_realtime_updates_single

app = Celery('tasks')

@app.task
def crawl_user(fbid, update_time)
    crawl_realtime_updates_singe(fbid, update_time)


if __name__ == '__main__':
    app.start()
