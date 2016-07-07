#!/usr/bin/env python
import os
import sys
import re

from subprocess import check_output

def main():
    res = check_output(['finger'])
    users = {}
    lines = res.strip().split('\n')
    fmt = re.compile(r'^([a-z0-9]+)\s([A-z\.\s]+)(pts\/\d+).*([A-z]{3}\s+\d{1,2}\s\d{2}:\d{2})')
    for line in lines[1:]:
        match = fmt.search(line)
        if not match:
            continue
        user, fullname, pts, login = match.groups()
        users[user] = [fullname.strip(), login, pts]

    for user in users:
        res = check_output(['groups', user])
        left = res.split(':')[-1].strip()
        group = left.split(' ')[0]
        users[user].insert(0, group)

    fmt = '{0:10} {1[0]:<14} {1[1]:<30} {1[2]:<15} {1[3]}'

    print
    print "Currently logged in users"
    print
    print fmt.format('user', ['group', 'fullname', 'logintime', 'tty'])
    for user, vals in sorted(users.iteritems(), key=lambda t: t[1][0]):
        print fmt.format(user, vals)
    print
if __name__ == "__main__":
    main()
