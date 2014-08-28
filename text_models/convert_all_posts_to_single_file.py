import os
import re

user_post_document_filename = '/data/topics/user_posts_from_individual/user-posts.txt'
data_dir = '/data/user_documents/all_originating_posts'

file = open(user_post_document_filename, 'w')
for filename in os.listdir(data_dir):
    cur_file = open(data_dir + '/' + filename)
    messages = ' '.join(re.split('\s+', cur_file.read()))
    file.write("{filename} {messages}\n".format(filename=filename, messages=messages))
    cur_file.close()
file.close()