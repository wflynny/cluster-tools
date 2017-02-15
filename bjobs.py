#!/usr/bin/env python
import re
import pwd, grp
import argparse
from operator import itemgetter
from subprocess import check_output
from colorama import Fore, Back, Style

def color_func(x, length=18, fill=' '):
    x = float(x)
    after = Fore.RESET
    before = Fore.YELLOW if 30 < x < 50 else \
                Fore.RED if x <= 30 else Fore.GREEN
    item = '{}{}{}'.format(before, x, after)
    return fill*(length - len(item)) + item

def main(args):
    if args.user or args.user == '':
        cmd_args = '-u {}'.format(args.user).strip()
    elif args.group or args.group == '':
        cmd_args = '-g {}'.format(args.group).strip()
    else: exit()
    res = check_output(['showstats', cmd_args])
    lines = res.strip().split('\n')[4:]

    getter = itemgetter(0, 5, 9, 7, -1)
    fmt = '{:>8.8}  {:>8.8}%  {:>8.8}%  {:>8.8}%  {:>8}%'
    header = ['name', 'jobs', 'cpu used', 'cpu reqd', 'wt acc']
    underline = '-'*52

    data = [list(getter(line.split())) for line in lines]
    if args.user or args.user == '':
        for row in data:
            g = grp.getgrgid(pwd.getpwnam(row[0]).pw_gid).gr_name
            row.insert(1, g)
        header.insert(1, 'group')
        fmt = '{:>8.8}  ' + fmt
        underline += '-'*10
    data.sort(key=lambda t: (t[1], t[0]))

    print fmt.format(*header)
    print underline
    for row in data:
        row[-1] = color_func(row[-1])
        print fmt.format(*row), Style.RESET_ALL

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-u', '--user', nargs='?', const='')
    group.add_argument('-g', '--group', nargs='?', const='')
    main(parser.parse_args())
