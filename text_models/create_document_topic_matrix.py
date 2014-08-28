from scipy.sparse import lil_matrix, csc_matrix
from scipy.io import mmwrite
import joblib
import re
import sys
import os

def get_document_to_topic_proportions_dict(topic_proportions_filename_list, strip_document_path=False):
    '''
    Given topic_proportions_filename, read in each filename and add to 
    a dictionary that stores filename -> topic -> proportion
    '''
    filename_to_topic_proportions = {} # filename -> topic -> proportion
    for topic_proportions_filename in topic_proportions_filename_list:
        topic_proportions_file =  open(topic_proportions_filename, 'r')
        for line in topic_proportions_file:
            line = line.strip()
            if line.startswith('#'):
                continue
            
            # parse out topic and proportion pairs
            # then parse out filename (which could contain whitespace)            
            topic_prop_pairs = re.findall('(?:\d+\s+0\.\d+)', line)
            topic_start_idx = re.search('(?:\d+\s+0\.\d+)', line).start()
            num_and_filename = line[:topic_start_idx].strip()
            filename_start_idx = re.search('(?:\d+)', num_and_filename).end()
            filename = num_and_filename[filename_start_idx:].strip()
            if filename.startswith('file:'):
                filename = filename[5:]
            if strip_document_path:
                filename = os.path.basename(filename)                

            filename_to_topic_proportions.setdefault(filename, {})
            for topic_prop_pair in topic_prop_pairs:
                vals = re.split('\s+', topic_prop_pair)
                topic_id = int(vals[0])
                proportion = float(vals[1])
                filename_to_topic_proportions[filename][topic_id] = proportion
        topic_proportions_file.close()
    return filename_to_topic_proportions

def get_document_to_topic_proportions_dict_with_label_list(topic_proportions_filename, label_list):
    '''
    This version only works with a single filename input
    Given topic_proportions_filename, read in each filename and add to  
    a dictionary that stores filename -> topic -> proportion
    label_list provides names for each line in the topic_proportions_file in the same order (helpful when null-source shows up)
    '''
    filename_to_topic_proportions = {} # filename -> topic -> proportion
    topic_proportions_file =  open(topic_proportions_filename, 'r')
    for label, line in zip(label_list, topic_proportions_file):
        line = line.strip()
        if line.startswith('#'):
            continue
        
        # parse out topic and proportion pairs
        # then parse out filename (which could contain whitespace)            
        topic_prop_pairs = re.findall('(?:\d+\s+0\.\d+)', line)
        topic_start_idx = re.search('(?:\d+\s+0\.\d+)', line).start()
        
        filename = label

        filename_to_topic_proportions.setdefault(filename, {})
        for topic_prop_pair in topic_prop_pairs:
            vals = re.split('\s+', topic_prop_pair)
            topic_id = int(vals[0])
            proportion = float(vals[1])
            filename_to_topic_proportions[filename][topic_id] = proportion
    topic_proportions_file.close()
    return filename_to_topic_proportions

def document_to_feature_matrix(documents, document_to_topic_proportions, num_topics, output_filename=None):
    features = lil_matrix((documents.shape[0], num_topics))
    for row_idx, document in enumerate(documents):
        if document in document_to_topic_proportions:
            for topic_id, proportion in document_to_topic_proportions[document].items():
                features[row_idx, topic_id] = proportion
    features = csc_matrix(features)
    if output_filename:
        mmwrite(output_filename, features)
    else:
        return features


if __name__ == '__main__':
    document_cache = sys.argv[1]
    topic_proportions_filename = sys.argv[2]
    num_topics = int(sys.argv[3])
    output_filename = sys.argv[4]
    if len(sys.argv) > 4:
        topic_proportions_loaded = sys.argv[4]
    else:
        topic_proportions_loaded = False

    documents = joblib.load(document_cache)

    # get all topics and proportions for each document
    if not topic_proportions_loaded:
        document_to_topic_proportions = get_document_to_topic_proportions_dict([topic_proportions_filename])
    else:
        document_to_topic_proportions = joblib.load(topic_proportions_filename)

    # for each filename in the input array of filenames, create rows in a feature matrix
    # and save to file
    document_to_feature_matrix(documents, document_to_topic_proportions, num_topics, output_filename)