import os

# Remove all files in directory that end with a bad set of characters
# Discarding non-text URLs (images, pdfs, ppts)
bad_file_extensions = ['.jpg', '.jpeg', '.gif', '.pdf', '.ppt', '.png']
data_dir = '/data/user_links'
for link in os.listdir(data_dir):
    if link[-4:] in bad_file_extensions or link[-3:] in bad_file_extensions:
        print('removing {}'.format(link))
        os.remove(data_dir + '/' + link)