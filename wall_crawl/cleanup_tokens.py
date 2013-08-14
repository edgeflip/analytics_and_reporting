#!/usr/bin/env python
from con_s3 import connect_s3
import json

def main():
    conn = connect_s3()
    tokens = conn.get_bucket('fbtokens')
    for each in tokens.list():
	content = each.get_content_as_string()
	content = json.loads(content)
	if "data" not in content.keys():
	    new_content = { "data": content[ content.keys()[0] ] }
	    each.set_contents_from_string(json.dumps(new_content))
	else:
	    pass

if __name__ == '__main__':
    main()
