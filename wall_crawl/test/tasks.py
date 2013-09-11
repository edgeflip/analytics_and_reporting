#!/usr/bin/env python
from __future__ import absolute_import

from test.celery import celery

@celery.task
def add(x, y):
    return x + y

@celery.task
def square(x):
    return x**2
    
