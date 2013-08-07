import requests

from raven import Client
#note: this key will need to go into a settings file
client = Client('http://3e7211bfc1644429a75338a5dbb90076:fce454c6031645ec93db8065d1e10679@localsentry.edgeflip.com/2')

headers = {'User-Agent':'EdgeMon'}

def ping(url, expected=200, auth=None):

    # stash these args to pass along to sentry if we need to
    extra = {'url':url, 'expected_status':expected}

    try:
        if not auth:
            response = requests.get(url)
        else:
            response = requests.get(url, auth=auth)

        if response.status_code != expected:
            client.captureMessage('Unexpected response {} from {}'.format(response.status_code, url), extra=extra)

    except:
        client.captureException(extra=extra)

if __name__=='__main__':
    ping('http://edgeflip.com', 200)
    ping('http://match.civisanalytics.com/match?first_name=Gregory&last_name=Valiant&city=Boston&state=MO', auth=('edgeflip','civis!19'))

