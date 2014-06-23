from get_user_links import get_canonical_link, get_filename_for_link
import os

# After removing ':' from canonical link filenames, there were duplicates.
# This script deletes them.
data_dir = '/data/user_links'
for link in os.listdir(data_dir):
    if ':' in link and os.path.isfile(data_dir + '/' + get_filename_for_link(get_canonical_link(link))):
        print('removing {}'.format(link))
        os.remove(data_dir + '/' + link)