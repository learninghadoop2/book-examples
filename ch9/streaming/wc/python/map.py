#!/bin/env python
import sys

for line in sys.stdin:
    # skip empty lines
    if line == '\n':
        continue

    # preserve utf-8 encoding
    try:
        line = line.encode('utf-8')
    except UnicodeDecodeError:
        continue
    # newline characters can appear within the text
    line = line.replace('\n', '')

    # lowercase and tokenize
    line = line.lower().split()

    for term in line:
        if not term:
            continue
        try:
            print(
                u"%s" % (
                    term.decode('utf-8')))
        except UnicodeEncodeError:
            continue
