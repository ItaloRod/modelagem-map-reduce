#!/usr/bin/env python
import sys
def output(previous_key, total):
    if previous_key != None:
        print(previous_key + ' was found ' + str(total) + ' times')
total = 0
previous_key = None


for line in sys.stdin:
    key, value = line.split('\t', 1)
    if key != previous_key:
        output(previous_key, total)
        previous_key = key
        total = 0
    total += int(value)
output(previous_key, total)