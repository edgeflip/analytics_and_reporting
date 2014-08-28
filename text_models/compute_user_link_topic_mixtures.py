from db_utils import redshift_connect, redshift_disconnect, execute_query
from get_user_links import get_canonical_link, get_filename_for_link
from create_document_topic_matrix import get_document_to_topic_proportions_dict
import joblib

def get_user_to_links(conn):
    query = """
            SELECT fbid_user, link
            FROM fbid_sample_50000_links
            """
    rows = execute_query(query, conn)
    
    user_to_links = {} # fbid_user -> [link, ...]
    for row in rows:
        fbid_user, link = row[0], get_filename_for_link(get_canonical_link(row[1]))
        user_to_links.setdefault(fbid_user, []).append(link)    
    return user_to_links

def get_user_to_topic_mixture(user_to_links, link_to_topic_proportions):
    '''
    Given a dict of users to a list of links and a dict of links to their topic proportions,
    build a dict of users to their topic mixture.    
    '''
    user_to_topic_proportions = {} # fbid_user -> topic -> proportion
    for user, links in user_to_links.items():
        num_valid_links = 0
        topic_to_proportions = {} # topic -> [proportion, ...]
        for link in links:
            if link in link_to_topic_proportions:
                num_valid_links += 1
                for topic, proportion in link_to_topic_proportions[link].items():                    
                    topic_to_proportions.setdefault(topic, []).append(proportion)
        topic_mixture = compute_topic_mixture(topic_to_proportions)
        user_to_topic_proportions[str(user)] = topic_mixture
    return user_to_topic_proportions

def compute_topic_mixture(topic_to_proportions):
    '''
    Given a dict of topic to a list of proportions and a total number of documents,
    compute a weighted mixture of each topic's contribution to the given corpus
    '''
    if topic_to_proportions:
#         total_weight = sum([sum(proportions) for proportions in topic_to_proportions.values()])
#         return {topic: 1.0*sum(proportions)/total_weight for topic, proportions in topic_to_proportions.items()}
        return {topic: max(proportions) for topic, proportions in topic_to_proportions.items()}
    else:
        return {}

if __name__ == '__main__':
    ## Parameters ##
    topic_proportions_filenames = ['/data/topics/user_links/user-links-train-1000-composition.txt',
                                   '/data/topics/user_links/user-links-inferred-1000-composition.txt']
    output_filename = '/data/topics/user_links/user-topic-proportions-max-cache.out'
                                   
    link_to_topic_proportions = get_document_to_topic_proportions_dict(topic_proportions_filenames, strip_document_path=True)
    
    conn = redshift_connect()
    user_to_links = get_user_to_links(conn)
    redshift_disconnect(conn)
    
    # get mixture of topics for each user based on their set of link documents
    user_to_topic_proportions = get_user_to_topic_mixture(user_to_links, link_to_topic_proportions)
    
    # output to a cached file
    joblib.dump(user_to_topic_proportions, output_filename)