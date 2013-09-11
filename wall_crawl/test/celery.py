from __future__ import absolute_import
from celery import Celery

celery = Celery('test.celery', broker='amqp://', include=['test.tasks'])

if __name__ == '__main__':
    celery.start()
