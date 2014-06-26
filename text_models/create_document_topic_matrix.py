from scipy.sparse import lil_matrix, csc_matrix
from scipy.io import mmwrite
import joblib
import sys

filename_cache = sys.argv[1]
topic_proportions_filename = sys.argv[2]
num_topics = int(sys.argv[3])
output_filename = sys.argv[4]

filenames = joblib.load(filename_cache)

# get all topics and proportions for each filename

filename_to_topic_proportions = {} # filename -> topic -> proportion
topic_proportions_file =  open(topic_proportions_filename, 'r')
for line in topic_proportions_file:
    line = line.strip()
    if line.startswith('#'):
        continue
    vals = line.strip().split('\t')
    filename = vals[1].split('file:')[1]
    filename_to_topic_proportions.setdefault(filename, {})
    for idx in range(2, len(vals), 2):
        topic_id = int(vals[idx])
        proportion = float(vals[idx+1])
        filename_to_topic_proportions[filename][topic_id] = proportion
topic_proportions_file.close()

# for each filename in the input array of filenames, create rows in a feature matrix

features = lil_matrix((filenames.shape[0], num_topics))
for row_idx, filename in enumerate(filenames):
    for topic_id, proportion in filename_to_topic_proportions[filename].items():
        features[row_idx, topic_id] = proportion

features = csc_matrix(features)
mmwrite(output_filename, features)