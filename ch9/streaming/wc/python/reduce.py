#!/bin/env python
import sys

count = 1
current = None

for word in sys.stdin:
    word = word.strip()

    if word == current:
        count += 1
    else:
        if current:
            print "%s\t%s" % (current.decode('utf-8'), count)
        current = word
        count = 1
if current == word:
    print "%s\t%s" % (current.decode('utf-8'), count)
