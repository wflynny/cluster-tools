#!/bin/bash
awk '{ print $2,int($1/1024/1024)}' /disk-1/diskhogs.txt | sort -grk2
