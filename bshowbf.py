#!/usr/bin/env python
from subprocess import check_output

res = check_output(['showbf', '-S']).split('\n')

for i, line in enumerate(res):
    if line.strip().startswith('gpu'): break
    print line

print '\t{} GPU nodes available'.format(len(filter(None, res[i:])))
