










def get_prim_sec_pairs_iter(bucket_names=AWS_BUCKET_NAMES, conn=None):
    if (conn is None):
        conn = get_conn_s3()
    file_count = 0
    bucket_count = 0
    for bucket_name in bucket_names:
        bucket = conn.get_bucket(bucket_name)
        bucket_count += 1
        for key in bucket.list():
            # name should have format primary_secondary; e.g., "100000008531200_1000760833"
            prim_id, sec_id = map(int, key.name.split("_"))
            file_count += 1
            yield (prim_id, sec_id)
    logger.debug("read %d s3 keys from %d buckets" % (file_count, bucket_count))







def pairs2dict(pairs, reverse=False):
    if reverse:
        pairs = [(t, s) for s, t in pairs]
    first__seconds = {}
    for s, t in pairs:
        first__seconds.setdefault(s, []).append(t)
    return first__seconds

def get_prim_sec_dict(bucket_names=AWS_BUCKET_NAMES, conn=None):
    return pairs2dict(get_prim_sec_pairs_iter(bucket_names, conn), reverse=False)

def get_sec_prim_dict(bucket_names=AWS_BUCKET_NAMES, conn=None):
    return pairs2dict(get_prim_sec_pairs_iter(bucket_names, conn), reverse=True)

def get_key_counts(json_list_of_dicts, print_posts=True, val_print_thresh_perc=0.01, val_print_thresh_num=2):
    key__count = {}
    key__val__count = {}

    domain__count = {}

    for json_dict in json_list_of_dicts:
        for k, v in json_dict.items():
            v = unidecode.unidecode(unicode(v))
            key__count[k] = key__count.get(k, 0) + 1
            if (k not in key__val__count):
                key__val__count[k] = {}
            key__val__count[k][v] = key__val__count[k].get(v, 0) + 1

        if ("link" in json_dict):
            domain = urlparse.urlparse(json_dict["link"]).hostname
            domain__count[domain] = domain__count.get(domain, 0) + 1


        # if ("likes" in json_dict):
        #     json_dict["likes"] = len(json_dict["likes"]["data"])
        # if ("comments" in json_dict):
        #     json_dict["comments"] = len(json_dict["comments"]["data"])

        if (print_posts):
            # print pretty_dict(json_dict) + "\n"
            print "######################################\n " + json.dumps(json_dict, ensure_ascii=True, indent=4) + "\n\n"


    sec = lambda x: x[1]

    for k, key_count in sorted(key__count.items(), key=sec, reverse=True):
        print "%-30s\t%s\t%.2f%%" % (str(k) + ":", str(key_count), 100.0*key_count/len(json_list_of_dicts))

        for v, val_count in sorted(key__val__count[k].items(), key=sec, reverse=True):
            if (val_count >= val_print_thresh_perc*len(json_list_of_dicts)) and (val_count >= val_print_thresh_num):
                print "\t\t\t\t%-75s\t%s\t%.2f%%" % (str(v)[:75] + ":", str(val_count), 100.0*val_count/len(json_list_of_dicts))

    domain_count_total = sum(domain__count.values())
    for domain, count in sorted(domain__count.items(), key=sec, reverse=True):
        print "%30s\t%d\t%.2f%%" % (domain, count, 100.0*count/domain_count_total)

def get_key_counts_by_type(json_list_of_dicts):
    # get all the types
    type_counts = {}
    for json_dict in json_list_of_dicts:
        post_type = json_dict["type"]
        type_counts[post_type] = type_counts.get(post_type, 0) + 1
    for post_type in type_counts.keys():
        json_list_of_dicts_type = [jd for jd in json_list_of_dicts if (jd["type"] == post_type)]
        print "######################################\nanalyzing %d posts of type: %s" % (len(json_list_of_dicts_type), post_type)
        get_key_counts(json_list_of_dicts_type, print_posts=False)
        print "\n\n\n"



def print_value_size_distrib(dict_guy):
    size__count = {}
    for v in dict_guy.values():
        size = len(v)
        size__count[size] = size__count.get(size, 0) + 1
    for size, count in sorted(size__count.items()):
        print "%d:\t%d" % (size, count)



###################################

if __name__ == '__main__':


    if (1):  # grab links from buckets

        prim__sec = get_prim_sec_dict()
        print "got %d primaries" % len(prim__sec)
        print "got %d secondaries" % sum([len(s) for s in prim__sec.values()])

        secs_unique = set()
        for secs in prim__sec.values():
            secs_unique.update(secs)
        print "got %d unique secondaries" % len(secs_unique)

        for k, v in prim__sec.items()[:30]:
            print "%30s:\t%s" % (k, str(v))
        print_value_size_distrib(prim__sec)


        sec__prim = get_prim_sec_dict()


    if (0):  # examine data from single aws key

        infile_name = sys.argv[1]
        infile = open(infile_name, 'r')

        json_all = json.load(infile)
        json_list_of_dicts = json_all['data']

        get_key_counts(json_list_of_dicts, print_posts=True)

        get_key_counts_by_type(json_list_of_dicts)





    if (0):  # grab the data

        import string
        trans_tab = string.maketrans("\t", " ")
        def get_text(dict_guy, keyj, default=""):
            if (key in dict_guy):
                return dict_guy[key].translate(trans_tab)
            else:
                return default

        outfile_objects = open("out_objects.tsv", 'wb')
        outfile_links = open("out_objects.tsv", 'wb')

        conn = get_conn_s3()
        for bucket_name in AWS_BUCKET_NAMES:

            print "processing bucket " + bucket_name

            bucket = conn.get_bucket(bucket_name)
            for key in bucket.list():

                print "\tprocessing key " + key.name

                # name should have format primary_secondary; e.g., "100000008531200_1000760833"
                prim_id, sec_id = map(int, key.name.split("_"))

                stuff = key.get_contents_as_string()

                print "\t\tgot %d stuff" % (len(stuff))
                print "\t\t\t" + stuff[:500]

                json_all = json.loads(key.get_contents_as_string())
                if ('data' not in json_all):
                    print "\t\t\tskipping key"
                    continue
                json_list_of_dicts = json_all['data']
                for json_dict in json_list_of_dicts:

                    try:
                        post_id = str(json_dict['id'])
                        post_ts = json_dict['updated_time']
                        post_type = json_dict['type']
                        post_app = json_dict['application']['id'] if 'application' in json_dict else ""

                        post_prim_id = str(prim_id)
                        post_sec_id = str(sec_id)

                        post_from = json_dict['from']['id'] if 'from' in json_dict else ""
                        post_link = json_dict.get('link', "")
                        post_link_domain = urlparse.urlparse(post_link).hostname if (post_link) else ""

                        post_story = get_text(json_dict, 'story')
                        post_description = get_text(json_dict, 'description')
                        post_caption = get_text(json_dict, 'caption')
                        post_message = get_text(json_dict, 'message')

                        out_fields = [post_id, post_ts, post_type, post_app,
                                      post_prim_id, post_sec_id, post_from,
                                      post_link, post_link_domain,
                                      post_story, post_description, post_caption, post_message]

                        line = "\t".join([f.encode('utf8', 'ignore') for f in out_fields])
                        print "writing line: " + line
                        outfile_objects.write(line + "\n")
                        print "wrote it"

                        to_ids = set()
                        like_ids = set()
                        comment_ids = set()
                        if ('to' in json_dict):
                            to_ids.update([ to['id'] for to in json_dict['to']['data'] ])
                        if ('likes' in json_dict):
                            like_ids.update([ liker['id'] for liker in json_dict['likes']['data'] ])
                        if ('comments' in json_dict):
                            comment_ids.update([ commer['id'] for commer in json_dict['comments']['data'] ])
                        for user_id in to_ids.union(like_ids, comment_ids):
                            has_to =  "1" if user_id in to_ids else ""
                            has_like = "1" if user_id in like_ids else ""
                            has_comm = "1" if user_id in comment_ids else ""
                            out_fields = [post_id, user_id, has_to, has_like, has_comm]
                            outfile_links.write("\t".join([f.encode('utf8', 'ignore') for f in out_fields]) + "\n")

                    except:
                        print json.dumps(json_dict, ensure_ascii=True, indent=4) + "\n"
                        print "post_id: " + post_id
                        print "post_ts: " + post_ts
                        print "post_type: " + post_type
                        print "post_app: " + post_app
                        print "post_prim_id: " + post_prim_id
                        print "post_sec_id: " + post_sec_id
                        print "post_from: " + post_from
                        print "post_link: " + post_link
                        print "post_link_domain: " + post_link_domain
                        print "post_story: " + post_story
                        print "post_description: " + post_description
                        print "post_caption: " + post_caption
                        print "post_message: " + post_message

                        raise

            sys.exit()


