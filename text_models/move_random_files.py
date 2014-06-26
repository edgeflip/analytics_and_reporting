import os
import random

sample_size = 99990
from_dir = '/data/user_links'
to_dir = '/data/user_links_train_topics'
all_links = os.listdir(from_dir)
random.shuffle(all_links)

for link in all_links[:sample_size]:
    os.rename(from_dir + '/' + link, to_dir + '/' + link)