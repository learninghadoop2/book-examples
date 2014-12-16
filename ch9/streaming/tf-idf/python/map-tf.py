#!/usr/bin/env python
# The mapper takes a json representaion of a tweet (doc) in stdin, tokenizes
# its text and emits a tuple "term<tab>doc_id".
# Test from the command line with
#
# $ head -n 5 tweets.json  | map-tf.py
#
# Tweet text is UTF-8 encoded. Make sure that PYTHONIOENCODING is set
# accordingly to pipe data in a unix terminal.
# export PYTHONIOENCODING='UTF-8'
import json
import sys

import logging
logger = logging.getLogger("")
logger.addHandler(
    logging.FileHandler("map-tf.log", mode='w', encoding="utf-8"))

for tweet in sys.stdin:
    # skip empty lines
    if tweet == '\n':
        continue
    try:
        tweet = json.loads(tweet)
    except:
        logger.warn("Invalid input %s " % tweet)
        continue
    # In our example one tweet corresponds to one document.
    doc_id = tweet['id_str']
    if not doc_id:
        continue

    # preserve utf-8 encoding
    text = tweet['text'].encode('utf-8')
    # newline characters can appear within the text
    text = text.replace('\n', '')

    # lowercase and tokenize
    text = text.lower().split()

    for term in text:
        try:
            print(
                u"%s\t%s" % (
                    term.decode('utf-8'), doc_id.decode('utf-8'))
                )
        except UnicodeEncodeError:
            # silently ignore utf-8 error - eg. when PYTHONIOENCODING is
            # not set
            logger.warn("Invalid term %s " % term)
