#!/usr/bin/env python
# In the reducer we count the number of times each unique (token, doc_id)
# pair appears. The resulting output will the term frequency (tf) metric.s
# Test from the command line with
#
# $ head -n 1000 tweets.json  |  map-tf.py  | sort -k1,2  | reduce-tf.py
#
# When testing, remember that the first two columns (the key) need to
# sorted (hadoop takes care of this when streaming).
import sys
import logging

logger = logging.getLogger("")
logger.addHandler(
    logging.FileHandler("reduce-tf.log", mode='w', encoding="utf-8"))

freq = 1
cur_term, cur_doc_id = sys.stdin.readline().split()
for line in sys.stdin:
    line = line.strip()
    try:
        term, doc_id = line.split('\t')
    except:
        logger.warn("Invalid record %s " % line)

    # the key is a (doc_id, term) pair
    if (doc_id == cur_doc_id) and (term == cur_term):
        freq += 1

    else:
        print(
            u"%s\t%s\t%s" % (
                cur_term.decode('utf-8'), cur_doc_id.decode('utf-8'), freq))
        cur_doc_id = doc_id
        cur_term = term
        freq = 1

print(
    u"%s\t%s\t%s" % (
        cur_term.decode('utf-8'), cur_doc_id.decode('utf-8'), freq))
