# Count the number of document (tweets) in a collection.
# In the mapper pahse we process one document at a time
# and emit a 'dummy' key.

import sys

for line in sys.stdin:
    # skip empty lines
    if line == '\n':
        continue
    print "%s\t%s" % ("num_doc", 1)
