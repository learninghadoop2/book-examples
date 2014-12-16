#!/usr/bin/env python
from __future__ import division
import sys
import math

import logging
logger = logging.getLogger("")
logger.addHandler(
    logging.FileHandler("tf-idf.log", mode='w', encoding="utf-8"))


# Number of documents in the collection.
# note that parametes can be passed via sys.argv
# also with hadoop streaming
num_doc = sys.argv[1]

for line in sys.stdin:
    line = line.strip()

    try:
        term, doc_id, tf, df = line.split('\t')

        tf = float(tf)
        df = float(df)
        num_doc = float(num_doc)
    except:
        logger.warn("Invalid record %s" % line)

    # idf = num_doc / df
    tf_idf = tf * (1+math.log(num_doc / df))
    print("%s\t%s\t%s" % (term, doc_id, tf_idf))
