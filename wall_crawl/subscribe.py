#!/usr/bin/env python


def subscribe_user(token,verify_token):
	api = 'https://graph.facebook.com/{0}'
	#api = 'https://graph.facebook.com/ <APP_ID>/subscriptions?object=feed&calback_url=<url>'
	#api = 'https://graph.facebook.com/{0}/subscriptions?object=feed&callback_url=http://ec2-50-16-226-172.compute-1.amazonaws.com'
	# api takes an {0} = appid {1} = access_token {2} = my EC2 instance DNS {3} a verify_token that I assign
	api = 'https://graph.facebook.com/{0}/subscriptions?access_token={1}&object=user&fields=feed&callback_url={2}&verify_token={3}'
