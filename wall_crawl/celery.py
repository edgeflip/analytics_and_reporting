#!/usr/bin/env python
from __future__ import absolute_import
from kombu import Queue

from celery import Celery

celery = Celery('wall_crawl.celery',
                broker='amqp://',
                backend='amqp://', 
                include=['wall_crawl.tasks'])

celery.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
    CELERY_QUEUES=(
		Queue('acx', routing_key='acx.db', queue_arguments=QUEUE_ARGS),
                Queue('crux', routing_key='crux.db', queue_arguments=QUEUE_ARGS),
    ),
)
