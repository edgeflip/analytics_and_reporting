### HOWTO run a periodic task with sentry/celery, if celery beat is running


from celery import Celery
C = Celery('monitoring', broker='django://')

import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'sentryconf'

from raven import Client
client = Client('http://3e7211bfc1644429a75338a5dbb90076:fce454c6031645ec93db8065d1e10679@localsentry.edgeflip.com/2')

from celery.task import periodic_task
from datetime import timedelta
@periodic_task(run_every=timedelta(minutes=1), name='raven_foo', queue='monitor' )
def raven():
    client.captureMessage('foo')
    
