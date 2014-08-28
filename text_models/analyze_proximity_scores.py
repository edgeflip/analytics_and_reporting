from db_utils import redshift_connect, redshift_disconnect, execute_query
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import train_test_split
from sklearn import metrics
import pandas as pd
import numpy as np
import random
import sys

# NOTES:
# friends that were selected, then unselected (6,936 times)
# friends that were unselected (after selected or select_all), then selected (2,468 times)
# friends that were manually selected, then manually unselected (48 times)
# friends that were manually unselected (presumably after first manually selecting them), then manually selected again (46 times)
# friends that were suppressed, then manually selected (3 times)
# how to keep track of which cases actually get an ultimate share or share to just others?

def get_edges_with_class_labels(conn):
    # only want edges where primary eventually shared, but not by selecting all 
    # (assume needs to be between 1 and 9 total shares)
    
    shared_with_query = \
            """
            SELECT t1.fbid, e.friend_fbid    
            FROM 
               (SELECT v.visit_id, fbid
                FROM visit_fbids v 
                    JOIN events_visitkey e ON v.visit_id = e.visit_id 
                GROUP BY v.visit_id, fbid
                HAVING 0 < sum(case when type = 'shared' then 1 else 0 end) and
                       sum(case when type = 'shared' then 1 else 0 end) < 4
                ) t1
                    JOIN events_visitkey e ON t1.visit_id = e.visit_id
            WHERE e.type = 'shared' and t1.fbid is not null and e.friend_fbid is not null
            """
    not_shared_with_query = \
            """
            SELECT t1.fbid, e.friend_fbid
            FROM
              (SELECT v.visit_id, fbid
                FROM visit_fbids v 
                    JOIN events_visitkey e ON v.visit_id = e.visit_id 
                GROUP BY v.visit_id, fbid
                HAVING 0 < sum(case when type = 'shared' then 1 else 0 end) and
                       sum(case when type = 'shared' then 1 else 0 end) < 4
                ) t1
                    JOIN events_visitkey e ON t1.visit_id = e.visit_id
            WHERE t1.fbid is not null and e.friend_fbid is not null
            GROUP BY t1.fbid, e.friend_fbid
            HAVING sum(case when type = 'shared' then 1 else 0 end) = 0 and
                   sum(case when type = 'shown' then 1 else 0 end) > 0
            """
    
    edge_to_class_labels = {} # (fbid_primary, fbid_secondary) -> [0,1]
    rows = execute_query(shared_with_query, conn)
    for row in rows:
        fbid_primary, fbid_secondary = map(int, row)
        edge_to_class_labels.setdefault((fbid_primary, fbid_secondary), 1)
    
    rows = execute_query(not_shared_with_query, conn)
    for row in rows:
        fbid_primary, fbid_secondary = map(int, row)
        if (fbid_primary, fbid_secondary) in edge_to_class_labels:
            sys.stderr.write('seen pair {} before...\n'.format((fbid_primary, fbid_secondary)))
        edge_to_class_labels.setdefault((fbid_primary, fbid_secondary), 0)
    return edge_to_class_labels

def get_edges_with_attributes(conn):
    query = """
            SELECT fbid_target as fbid_primary, fbid_source as fbid_secondary, 
                   coalesce(post_likes, 0), coalesce(post_comms, 0), 
                   coalesce(stat_likes, 0), coalesce(stat_comms, 0), 
                   coalesce(wall_posts, 0), coalesce(wall_comms, 0), coalesce(tags, 0),
                   coalesce(photos_target, 0), coalesce(photos_other, 0), 
                   coalesce(mut_friends, 0)
            FROM visit_edges_with_attributes
            WHERE fbid_target is not null and fbid_source is not null and post_likes is not null
            """
    edge_to_attributes = {}  # (fbid_primary, fbid_secondary) -> [post_likes, ..., mut_friends]
    rows = execute_query(query, conn)
    for row in rows:
        vals = map(int, row)
        fbid_primary, fbid_secondary = vals[:2]
        edge_to_attributes[(fbid_primary, fbid_secondary)] = vals[2:]
    return edge_to_attributes

def get_primary_normalizers(conn):
    query = """
            SELECT fbid_target, coalesce(max_post_likes, 0), coalesce(max_post_comms, 0), 
                   coalesce(max_stat_likes, 0), coalesce(max_stat_comms, 0), 
                   coalesce(max_wall_posts, 0), coalesce(max_wall_comms, 0), 
                   coalesce(max_tags, 0), coalesce(max_photos_target, 0), 
                   coalesce(max_photos_other, 0), coalesce(max_mut_friends, 0)
            FROM fbid_max_edge_attributes
            WHERE fbid_target is not null and max_post_likes is not null
            """
    primary_to_max_attributes = {}  # fbid_primary, -> [max_post_likes, ..., max_mut_friends]
    rows = execute_query(query, conn)
    for row in rows:
        fbid_primary = int(row[0])
        primary_to_max_attributes[fbid_primary] = map(float, row[1:])
    return primary_to_max_attributes

def get_primary_standardizers(conn):
    query = """
            SELECT fbid_target, coalesce(avg_post_likes, 0), coalesce(avg_post_comms, 0), 
                   coalesce(avg_stat_likes, 0), coalesce(avg_stat_comms, 0), 
                   coalesce(avg_wall_posts, 0), coalesce(avg_wall_comms, 0), 
                   coalesce(avg_tags, 0), coalesce(avg_photos_target, 0), 
                   coalesce(avg_photos_other, 0), coalesce(avg_mut_friends, 0),
                   coalesce(stddev_post_likes, 0), coalesce(stddev_post_comms, 0), 
                   coalesce(stddev_stat_likes, 0), coalesce(stddev_stat_comms, 0), 
                   coalesce(stddev_wall_posts, 0), coalesce(stddev_wall_comms, 0), 
                   coalesce(stddev_tags, 0), coalesce(stddev_photos_target, 0), 
                   coalesce(stddev_photos_other, 0), coalesce(stddev_mut_friends, 0)
            FROM fbid_avg_stddev_edge_attributes
            WHERE fbid_target is not null and avg_post_likes is not null
            """
    primary_to_avg_stddev_attributes = {}  # fbid_primary, -> [avg_post_likes, ..., stddev_mut_friends]
    rows = execute_query(query, conn)
    for row in rows:
        fbid_primary = int(row[0])
        primary_to_avg_stddev_attributes[fbid_primary] = map(float, row[1:])
    return primary_to_avg_stddev_attributes

if __name__ == '__main__':
    conn = redshift_connect()
    
    edge_to_class_labels = get_edges_with_class_labels(conn)
    class_data = [(primary, secondary, class_label) for (primary, secondary), class_label in edge_to_class_labels.items()]
    random.shuffle(class_data)
    class_col_names = ['fbid_primary', 'fbid_secondary', 'class_label']
    class_df = pd.DataFrame({col_name: [x[i] for x in class_data] for i, col_name in enumerate(class_col_names)})
    # print(class_df.describe())
    # print(class_df)
    
    # get edges and px3/px4 input values
    edge_to_attributes = get_edges_with_attributes(conn)
    attr_data = [(primary, secondary) + tuple(attributes) for (primary, secondary), attributes in edge_to_attributes.items()]    
    attr_col_names = ['fbid_primary', 'fbid_secondary', 'post_likes', 'post_comms', 'stat_likes', 
                      'stat_comms', 'wall_posts', 'wall_comms', 'tags', 'photos_target', 
                      'photos_other', 'mut_friends']    
    attr_df = pd.DataFrame({col_name: [x[i] for x in attr_data] for i, col_name in enumerate(attr_col_names)})
    # print(attr_df.describe())
    # print(attr_df)
    
    class_and_attr_df = pd.merge(class_df, attr_df, how='inner', on=['fbid_primary', 'fbid_secondary'])
    # get normalizers
    primary_to_max_attributes = get_primary_normalizers(conn)
    max_attr_data = [(primary,) + tuple(max_attributes) for primary, max_attributes in primary_to_max_attributes.items()]    
    max_attr_col_names = ['fbid_primary', 'max_post_likes', 'max_post_comms', 'max_stat_likes', 
                      'max_stat_comms', 'max_wall_posts', 'max_wall_comms', 'max_tags', 
                      'max_photos_target', 'max_photos_other', 'max_mut_friends']    
    max_attr_df = pd.DataFrame({col_name: [x[i] for x in max_attr_data] for i, col_name in enumerate(max_attr_col_names)})
    class_attr_and_max_attr_df = pd.merge(class_and_attr_df, max_attr_df, how='inner', on='fbid_primary')
    class_attr_and_max_attr_df.to_csv('targeted_sharing_attrs_max.tsv', sep='\t', header=True)
    
    # get standardizers
    primary_to_avg_stddev_attributes = get_primary_standardizers(conn)
    avg_stddev_attr_data = [(primary,) + tuple(avg_stddev_attributes) for primary, avg_stddev_attributes in primary_to_avg_stddev_attributes.items()]
    avg_stddev_attr_col_names = ['fbid_primary', 'avg_post_likes', 'avg_post_comms', 'avg_stat_likes', 
                      'avg_stat_comms', 'avg_wall_posts', 'avg_wall_comms', 'avg_tags', 
                      'avg_photos_target', 'avg_photos_other', 'avg_mut_friends', 
                      'stddev_post_likes', 'stddev_post_comms', 'stddev_stat_likes', 
                      'stddev_stat_comms', 'stddev_wall_posts', 'stddev_wall_comms', 'stddev_tags', 
                      'stddev_photos_target', 'stddev_photos_other', 'stddev_mut_friends']
    avg_stddev_attr_df = pd.DataFrame({col_name: [x[i] for x in avg_stddev_attr_data] for i, col_name in enumerate(avg_stddev_attr_col_names)})
    class_attr_and_avg_stddev_attr_df = pd.merge(class_and_attr_df, avg_stddev_attr_df, how='inner', on='fbid_primary')
    class_attr_and_avg_stddev_attr_df.to_csv('targeted_sharing_attrs_avg_stds.tsv', sep='\t', header=True)
    
    shuffled_idxs = range(len(class_and_attr_df))
    random.shuffle(shuffled_idxs)
    class_and_attr_df = class_and_attr_df.ix[shuffled_idxs]
    y = class_and_attr_df['class_label']
    X_px3 = class_and_attr_df[['mut_friends', 'photos_target', 'photos_other']]
    X_px4 = class_and_attr_df[['mut_friends', 'photos_target', 'photos_other', 'post_likes', 
                               'post_comms', 'stat_likes', 'stat_comms', 'wall_posts', 
                               'wall_comms', 'tags']]
    
    px3_norm_col_names = ['mut_friends', 'photos_target', 'photos_other']
    norm_data = {}
    for norm_col_name in px3_norm_col_names:
        normalized_col = 1.0*class_attr_and_max_attr_df[norm_col_name] / class_attr_and_max_attr_df['max_'+ norm_col_name]
        normalized_col[normalized_col == np.inf] = 0.0
        normalized_col[np.isnan(normalized_col)] = 0.0
        norm_data[norm_col_name + '_max_norm'] = normalized_col
        print(np.max(normalized_col))
    norm_data['class_label'] = class_attr_and_max_attr_df['class_label']
    max_norm_df = pd.DataFrame(norm_data)
    
    shuffled_idxs = range(len(max_norm_df))
    random.shuffle(shuffled_idxs)
    max_norm_df = max_norm_df.ix[shuffled_idxs]
    
    X_px3_max_norm = max_norm_df[[col for col in max_norm_df.columns if col != 'class_label']]
    y_px3_max_norm = max_norm_df['class_label']
    
    px4_norm_col_names = ['mut_friends', 'photos_target', 'photos_other','post_likes', 
                          'post_comms', 'stat_likes', 'stat_comms', 'wall_posts', 
                          'wall_comms', 'tags']
    norm_data = {}
    for norm_col_name in px4_norm_col_names:
        normalized_col = 1.0*class_attr_and_max_attr_df[norm_col_name] / class_attr_and_max_attr_df['max_'+ norm_col_name]
        normalized_col[normalized_col == np.inf] = 0.0
        normalized_col[np.isnan(normalized_col)] = 0.0
        norm_data[norm_col_name + '_max_norm'] = normalized_col
    norm_data['class_label'] = class_attr_and_max_attr_df['class_label']
    max_norm_df = pd.DataFrame(norm_data)
    
    shuffled_idxs = range(len(max_norm_df))
    random.shuffle(shuffled_idxs)
    max_norm_df = max_norm_df.ix[shuffled_idxs]
    
    X_px4_max_norm = max_norm_df[[col for col in max_norm_df.columns if col != 'class_label']]
    y_px4_max_norm = max_norm_df['class_label']
    
    px3_stand_col_names = ['mut_friends', 'photos_target', 'photos_other']
    stand_data = {}
    for stand_col_name in px3_stand_col_names:
        standardized_col = 1.0*(class_attr_and_avg_stddev_attr_df[stand_col_name] - class_attr_and_avg_stddev_attr_df['avg_'+ stand_col_name]) / class_attr_and_avg_stddev_attr_df['stddev_'+ stand_col_name]
        standardized_col[standardized_col == np.inf] = 0.0
        standardized_col[np.isnan(standardized_col)] = 0.0
        stand_data[stand_col_name + '_stand'] = standardized_col
    stand_data['class_label'] = class_attr_and_avg_stddev_attr_df['class_label']
    stand_df = pd.DataFrame(stand_data)
    
    shuffled_idxs = range(len(stand_df))
    random.shuffle(shuffled_idxs)
    stand_df = stand_df.ix[shuffled_idxs]
    
    X_px3_stand = stand_df[[col for col in stand_df.columns if col != 'class_label']]
    y_px3_stand = stand_df['class_label']
    
    px4_stand_col_names = ['mut_friends', 'photos_target', 'photos_other','post_likes', 
                          'post_comms', 'stat_likes', 'stat_comms', 'wall_posts', 
                          'wall_comms', 'tags']
    stand_data = {}
    for stand_col_name in px4_stand_col_names:
        standardized_col = 1.0*(class_attr_and_avg_stddev_attr_df[stand_col_name] - class_attr_and_avg_stddev_attr_df['avg_'+ stand_col_name]) / class_attr_and_avg_stddev_attr_df['stddev_'+ stand_col_name]
        standardized_col[standardized_col == np.inf] = 0.0
        standardized_col[np.isnan(standardized_col)] = 0.0
        stand_data[stand_col_name + '_stand'] = standardized_col
    stand_data['class_label'] = class_attr_and_avg_stddev_attr_df['class_label']
    stand_df = pd.DataFrame(stand_data)

    shuffled_idxs = range(len(stand_df))
    random.shuffle(shuffled_idxs)
    stand_df = stand_df.ix[shuffled_idxs]    
    
    X_px4_stand = stand_df[[col for col in stand_df.columns if col != 'class_label']]
    y_px4_stand = stand_df['class_label']
    
    # split into train/test sets
    X_px3_train, X_px3_test, y_px3_train, y_px3_test = train_test_split(X_px3, y, test_size=0.2, random_state=42)
    X_px4_train, X_px4_test, y_px4_train, y_px4_test = train_test_split(X_px4, y, test_size=0.2, random_state=42)
    X_px3_max_norm_train, X_px3_max_norm_test, y_px3_max_norm_train, y_px3_max_norm_test = train_test_split(X_px3_max_norm, y_px3_max_norm, test_size=0.2, random_state=42)
    X_px4_max_norm_train, X_px4_max_norm_test, y_px4_max_norm_train, y_px4_max_norm_test = train_test_split(X_px4_max_norm, y_px4_max_norm, test_size=0.2, random_state=42)
    X_px3_stand_train, X_px3_stand_test, y_px3_stand_train, y_px3_stand_test = train_test_split(X_px3_stand, y_px3_stand, test_size=0.2, random_state=42)
    X_px4_stand_train, X_px4_stand_test, y_px4_stand_train, y_px4_stand_test = train_test_split(X_px4_stand, y_px4_stand, test_size=0.2, random_state=42)
    print('training with {} instances, testing on {} instances'.format(len(X_px3_train), len(y_px3_test)))
    
    # get class prior
    print('class prior:')
    print(np.mean(y_px3_test))
    
    # train models (px3/px4 unnormalized, normalized with max, standardized with mean and std)
    clf = LogisticRegression()
    clf.fit(X_px3_train, y_px3_train)
    preds_px3 = clf.predict(X_px3_test)
    print('px3 (unnormalized) accuracy:')
    print(metrics.accuracy_score(y_px3_test, preds_px3))
    print(metrics.classification_report(y_px3_test, preds_px3))
    print(metrics.confusion_matrix(y_px3_test, preds_px3))
    
    clf = LogisticRegression()
    clf.fit(X_px4_train, y_px4_train)
    preds_px4 = clf.predict(X_px4_test)
    print('px4 (unnormalized) accuracy:')
    print(metrics.accuracy_score(y_px4_test, preds_px4))
    print(metrics.classification_report(y_px4_test, preds_px4))
    print(metrics.confusion_matrix(y_px4_test, preds_px4))
    
    clf = LogisticRegression()
    clf.fit(X_px3_max_norm_train, y_px3_max_norm_train)
    preds_px3_max_norm = clf.predict(X_px3_max_norm_test)
    print('px3 (normalized with max) accuracy:')
    print(metrics.accuracy_score(y_px3_max_norm_test, preds_px3_max_norm))
    print(metrics.classification_report(y_px3_max_norm_test, preds_px3_max_norm))
    print(metrics.confusion_matrix(y_px3_max_norm_test, preds_px3_max_norm))
    
    clf = LogisticRegression()
    clf.fit(X_px4_max_norm_train, y_px4_max_norm_train)
    preds_px4_max_norm = clf.predict(X_px4_max_norm_test)
    print('px4 (normalized with max) accuracy:')
    print(metrics.accuracy_score(y_px4_max_norm_test, preds_px4_max_norm))
    print(metrics.classification_report(y_px4_max_norm_test, preds_px4_max_norm))
    print(metrics.confusion_matrix(y_px4_max_norm_test, preds_px4_max_norm))
    
    clf = LogisticRegression()
    clf.fit(X_px3_stand_train, y_px3_stand_train)
    preds_px3_stand = clf.predict(X_px3_stand_test)
    print('px3 (standardized) accuracy:')
    print(metrics.accuracy_score(y_px3_stand_test, preds_px3_stand))
    print(metrics.classification_report(y_px3_stand_test, preds_px3_stand))
    print(metrics.confusion_matrix(y_px3_stand_test, preds_px3_stand))    
    
    clf = LogisticRegression()
    clf.fit(X_px4_stand_train, y_px4_stand_train)
    preds_px4_stand = clf.predict(X_px4_stand_test)
    print('px4 (standardized) accuracy:')
    print(metrics.accuracy_score(y_px4_stand_test, preds_px4_stand))
    print(metrics.classification_report(y_px4_stand_test, preds_px4_stand))
    print(metrics.confusion_matrix(y_px4_stand_test, preds_px4_stand))
    
    redshift_disconnect(conn)