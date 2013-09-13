#!/usr/bin/env python
from __future__ import absolute_import
from kombu import Queue

from celery import Celery
from celery.utils.logger import get_task_logger
from celery import group, chain, chunks

celery = Celery(broker='amqp://',
                backend='amqp://')


@celery.task
def add(x, y):
    return x + y


if __name__ == '__main__':
    celery.start()

#celery.conf.update(
#    CELERY_TASK_RESULT_EXPIRES=3600,
#    CELERY_QUEUES=(
#		Queue('acx', routing_key='acx.db', queue_arguments=QUEUE_ARGS),
#                Queue('crux', routing_key='crux.db', queue_arguments=QUEUE_ARGS),
#    ),
#)
