#!/usr/bin/env python
import re
import pwd, grp
import argparse
from operator import itemgetter
from subprocess import check_output
from colorama import Fore, Back, Style

def color_func(x):
    x = float(x)
    if x < 30.:
        return '{}{}{}'.format(Fore.RED, x, Fore.RESET)
    if x >= 50.:
        return '{}{}{}'.format(Fore.GREEN, x, Fore.RESET)
    else:
        return '{}{}{}'.format(Fore.YELLOW, x, Fore.RESET)

def main(args):
    if args.user or args.user == '':
        cmd_args = '-u {}'.format(args.user).strip()
    elif args.group or args.group == '':
        cmd_args = '-g {}'.format(args.group).strip()
    else: exit()
    res = check_output(['showstats', cmd_args])
    lines = res.strip().split('\n')[4:]

    getter = itemgetter(0, 5, 9, 7, -1)
    fmt = '{:>8.8}  {:>8.8}%  {:>8.8}%  {:>8.8}%  {:>8.8}%'
    header = ('name', 'jobs', 'cpu used', 'cpu reqd', 'wt acc')

    data = [list(getter(line.split())) for line in lines]
    data.sort()

    print fmt.format(*header)
    print '-'*52
    for row in data:
        row[-1] = color_func(row[-1])
        print fmt.format(*row), Style.RESET_ALL

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-u', '--user', nargs='?', const='')
    group.add_argument('-g', '--group', nargs='?', const='')
    main(parser.parse_args())
