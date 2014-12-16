# Count the number of document (tweets) in a collection.
# In the reducer we group together occurrencies of the
# dummy key.

import sys

count = 0
for line in sys.stdin:
    key, value = line.split('\t')
    if key == 'num_doc':
        count += 1

print("%s" % count)
