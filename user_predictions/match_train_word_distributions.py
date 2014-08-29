import random
import sys
import re

import numpy as np

from predict_utils import get_ids_from_file, get_user_posts, order_user_posts

pos_label = sys.argv[1]
neg_label = sys.argv[2]

pos_dir = '/data/user_documents/individual_posts_{}'.format(pos_label)
neg_dir = '/data/user_documents/individual_posts_{}'.format(neg_label)

sys.stdout.write('loading ids\n')
pos_train_ids_filename = pos_dir + '/' + 'train-user-ids.txt'
pos_train_ids = get_ids_from_file(pos_train_ids_filename)
neg_train_ids_filename = neg_dir + '/' + 'train-user-ids.txt'
neg_train_ids = get_ids_from_file(neg_train_ids_filename)

sys.stdout.write('loading positive posts\n')
pos_post_filename = pos_dir + '/' + 'all-individual-posts.txt'
pos_aboutme_filename = pos_dir + '/' + 'all-individual-aboutme.txt'
pos_train_user_posts = get_user_posts(pos_post_filename, 
                                      pos_aboutme_filename,
                                      set(pos_train_ids))[0]

sys.stdout.write('loading negative posts\n')
neg_post_filename = neg_dir + '/' + 'all-individual-posts.txt'
neg_aboutme_filename = neg_dir + '/' + 'all-individual-aboutme.txt'
neg_train_user_posts = get_user_posts(neg_post_filename, 
                                      neg_aboutme_filename,
                                      set(neg_train_ids))[0]

pos_train_user_posts = order_user_posts(pos_train_user_posts, pos_train_ids)
neg_train_user_posts = order_user_posts(neg_train_user_posts, neg_train_ids)

tok = re.compile(r'(?u)\b\w\w+\b')

sys.stdout.write('counting words in positive posts\n')
pos_fbid_to_word_counts = {} # fbid -> (post_words, aboutme_words)
pos_post_word_counts = []
pos_aboutme_word_counts = []
for fbid, user_post in zip(pos_train_ids, pos_train_user_posts):
    post_word_count = len(tok.findall(user_post.posts.lower()))
    post_aboutme_count = len(tok.findall(user_post.aboutme.lower()))
    pos_fbid_to_word_counts[fbid] = (post_word_count, post_aboutme_count)
    pos_post_word_counts.append(post_word_count)
    pos_aboutme_word_counts.append(post_aboutme_count)

sys.stdout.write('counting words in negative posts\n')
neg_fbid_to_word_counts = {} # fbid -> (post_words, aboutme_words)
neg_post_word_counts = []
neg_aboutme_word_counts = []
for fbid, user_post in zip(neg_train_ids, neg_train_user_posts):
    post_word_count = len(tok.findall(user_post.posts.lower()))
    post_aboutme_count = len(tok.findall(user_post.aboutme.lower()))
    neg_fbid_to_word_counts[fbid] = (post_word_count, post_aboutme_count)
    neg_post_word_counts.append(post_word_count)
    neg_aboutme_word_counts.append(post_aboutme_count)

sys.stdout.write('creating 2d frequency distribution\n')
# Create a 2d frequency distribution for positive set (discretized to 20x20?)
post_bins = np.array([np.percentile(pos_post_word_counts, perc) for perc in range(5,101, 5)])
aboutme_bins = np.array([np.percentile(pos_aboutme_word_counts, perc) for perc in range(5,101, 5)])

# Assign bins to positive fbids
pos_post_bin_assns = np.digitize(pos_post_word_counts, post_bins)
pos_aboutme_bin_assns = np.digitize(pos_aboutme_word_counts, aboutme_bins)

# fbid_to_bin, a dict of fbid -> (x, y)
pos_fbid_to_bin = {pos_fbid: (pos_post_bin_assn, pos_aboutme_bin_assn) for 
                    pos_fbid, pos_post_bin_assn, pos_aboutme_bin_assn in
                    zip(pos_train_ids, pos_post_bin_assns, pos_aboutme_bin_assns)}

# Get relative frequency of each bin
bin_to_freq_pos = {} # (x, y) -> freq
bin_to_pos_fbids = {} # (x, y) -> [fbid...]
pos_total = len(pos_fbid_to_word_counts)
for pos_fbid, bin in pos_fbid_to_bin.items():
    bin_to_freq_pos.setdefault(bin, 0)
    bin_to_pos_fbids.setdefault(bin, [])
    bin_to_freq_pos[bin] += 1
    bin_to_pos_fbids[bin].append(pos_fbid)

# convert counts to relative frequencies
bin_to_freq_pos = {bin: 1.0*freq/pos_total for bin, freq in bin_to_freq_pos.items()}

# Assign bins to negative fbids
neg_post_bin_assns = np.digitize(neg_post_word_counts, post_bins)
neg_aboutme_bin_assns = np.digitize(neg_aboutme_word_counts, aboutme_bins)

neg_fbid_to_bin = {neg_fbid: (neg_post_bin_assn, neg_aboutme_bin_assn) for 
                    neg_fbid, neg_post_bin_assn, neg_aboutme_bin_assn in
                    zip(neg_train_ids, neg_post_bin_assns, neg_aboutme_bin_assns)}

# Get relative frequency of each bin for negative users
bin_to_freq_neg = {} # (x, y) -> freq
bin_to_neg_fbids = {} # (x, y) -> [fbid...]
neg_total = len(neg_fbid_to_word_counts)
for neg_fbid, bin in neg_fbid_to_bin.items():
    bin_to_freq_neg.setdefault(bin, 0)
    bin_to_neg_fbids.setdefault(bin, [])
    bin_to_freq_neg[bin] += 1
    bin_to_neg_fbids[bin].append(neg_fbid)

# convert counts to relative frequencies
bin_to_freq_neg = {bin: 1.0*freq/neg_total for bin, freq in bin_to_freq_neg.items()}

# Go through each bin, get the minimum relative frequency between pos and neg,
# and sample down accordingly.
pos_keep_ids = []
neg_keep_ids = []
for bin in bin_to_freq_pos:
    pos_freq = bin_to_freq_pos[bin]
    neg_freq = 0.0 if bin not in bin_to_freq_neg else bin_to_freq_neg[bin]
    if pos_freq == 0 or neg_freq == 0:
        continue
    
    min_freq = min(pos_freq, neg_freq)
    
    pos_ids = bin_to_pos_fbids[bin]
    pos_keep_ids.extend(random.sample(pos_ids, int(len(pos_ids)*min_freq/pos_freq)))
    neg_ids = [] if bin not in bin_to_neg_fbids else bin_to_neg_fbids[bin]
    neg_keep_ids.extend(random.sample(neg_ids, int(len(neg_ids)*min_freq/neg_freq)))

sys.stdout.write('Retained {} positives\n'.format(len(pos_keep_ids)))
new_pos_train_ids_filename = pos_dir + '/' + 'train-user-ids-sample.txt'
f = open(new_pos_train_ids_filename, 'w')
for pos_id in pos_keep_ids:
    f.write('{}\n'.format(pos_id))
f.close()

sys.stdout.write('Retained {} negatives\n'.format(len(neg_keep_ids)))
new_neg_train_ids_filename = neg_dir + '/' + \
                                'train-user-ids-sample-for-{}.txt'.format(pos_label)
f = open(new_neg_train_ids_filename, 'w')
for pos_id in neg_keep_ids:
    f.write('{}\n'.format(pos_id))
f.close()