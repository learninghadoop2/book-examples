#!/usr/bin/env python
# Test with
# cat <tf-output> | map-df.py  | sort -k2 | reduce-df.py
#
# note that we are sorting only on the term

import sys
import logging
logger = logging.getLogger("")
logger.addHandler(
    logging.FileHandler("reduce-tf.log", mode='w', encoding="utf-8"))

# Cache the (term,doc_id, tf) tuple. This is needed to associate a df
# to each words across multuple documents
key_cache = []

line = sys.stdin.readline().strip()
cur_term, cur_doc_id, cur_tf = line.split('\t')
cur_tf = int(cur_tf)
cur_df = 1

for line in sys.stdin:
    line = line.strip()

    try:
        term, doc_id, tf = line.strip().split('\t')
        tf = int(tf)
    except:
        logger.warn("Invalid record: %s " % line)
        continue

    # term is the only key for this input
    if (term == cur_term):
        # increment document frequency
        cur_df += 1

        # keep track of (term, doc_id, tf); df must be printed for all doc ids
        key_cache.append(
            u"%s\t%s\t%s" % (term.decode('utf-8'), doc_id.decode('utf-8'), tf))

    else:
        for key in key_cache:
            print("%s\t%s" % (key, cur_df))

        print (
            u"%s\t%s\t%s\t%s" % (
                cur_term.decode('utf-8'),
                cur_doc_id.decode('utf-8'),
                cur_tf, cur_df)
            )

        # flush the cache
        key_cache = []
        cur_doc_id = doc_id
        cur_term = term
        cur_tf = tf
        cur_df = 1

for key in key_cache:
    print(u"%s\t%s" % (key.decode('utf-8'), cur_df))
print(
    u"%s\t%s\t%s\t%s\n" % (
        cur_term.decode('utf-8'),
        cur_doc_id.decode('utf-8'),
        cur_tf, cur_df))
