#!/usr/bin/env python
# Note that this is a use case for
# the stock IdentityMapper.

# cat <tf-output> | map-df.py

import sys
import logging

logger = logging.getLogger("")
logger.addHandler(
    logging.FileHandler("map-df.log", mode='w', encoding="utf-8"))

for line in sys.stdin:
    try:
        term, doc_id, tf = line.strip().split('\t')
        print(
            u"%s\t%s\t%s" % (term.decode('utf-8'), doc_id.decode('utf-8'), tf))

    except:
        logger.warn("Invalid record: %s" % line)
