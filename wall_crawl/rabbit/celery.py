#!/usr/bin/env python
from kombu import Exchange, Queue


CELERY_DEFAULT_QUEUE = 'default'
CELERY_QUEUES = (
	Queue('default', Exchange('default'), routing_key='default'),
)


