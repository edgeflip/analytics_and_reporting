run = False
if run:
    data_dir = '/data/topics/individual_posts'
    topic_word_unnormalized_weights_filename = 'individual-posts-default-regex-1000-topic-word-weights.txt'
    topic_word_normalized_weights_filename = 'individual-posts-default-regex-1000-topic-word-normalized-weights.txt'
    in_file = open(data_dir + '/' + topic_word_unnormalized_weights_filename, 'r')
    out_file = open(data_dir + '/' + topic_word_normalized_weights_filename, 'w')

    beta_threshold = 0.0005
    cur_topic = -1
    cur_total_weight = 0.0
    word_to_weight = {}
    for line in in_file:
        topic, word, weight = line.strip().split('\t')
        topic = int(topic)
        weight = float(weight)
        if weight < beta_threshold:
            continue
    
        if topic != cur_topic and cur_topic == -1:
            # initialize
            cur_topic = topic
            cur_total_weight += weight
            word_to_weight[word] = weight
        elif topic != cur_topic and cur_topic != -1:
            # dump topic words to file
            for word, weight in word_to_weight.items():
                out_file.write('{}\t{}\t{}\n'.format(cur_topic, word, 1.0*weight/cur_total_weight))
            out_file.flush()
            # reset
            cur_topic = topic
            word_to_weight = {}
            cur_total_weight = 0.0
        else:
            # another word in same topic, add on
            cur_total_weight += weight
            word_to_weight[word] = weight
    # dump final topic
    for word, weight in word_to_weight.items():
        out_file.write('{}\t{}\t{}\n'.format(cur_topic, word, 1.0*weight/cur_total_weight))
    out_file.flush()

    in_file.close()
    out_file.close()