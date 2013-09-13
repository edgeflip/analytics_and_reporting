from __future__ import absolute_import
from celery import Celery

#celery = Celery('test.celery', broker='amqp://', include=['test.tasks'])
app = Celery()
import celeryconfig
app.config_from_object(celeryconfig)

if __name__ == '__main__':
    app.start()
