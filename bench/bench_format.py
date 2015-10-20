#!/usr/bin/python

import uncertainties
import numpy
import sys

def filter_outliers(numbers, mean, std):
    return filter(lambda x: abs(x - mean) < 2 * std, numbers)

op_str = {
    'create_files' : 'Create 100 files',
    'rm_files' : 'Unlink 100 files',
    'create_files_parallel' : 'Create 100 files (parallel)',
    'rm_files_parallel' : 'Unlink 100 files (parallel)',
    'ls_files' : 'ls with 1000 files',
    'write_large_file' : 'Write 1GB',
    'read_large_file' : 'Read 1GB',
    'read_first_byte' : 'Time to 1st byte',
}

f = sys.argv[1]
data = open(f).readlines()
print 'operation |  s3fs  | goofys | speedup'
print '----------| ------ | ------ | -------'
for l in data:
    nums = l.strip().split('\t')
    op = op_str[nums[0]]
    x = map(lambda x: float(x), nums[1].strip().split(' '))
    y = map(lambda x: float(x), nums[2].strip().split(' '))
    mean_x = numpy.mean(x)
    err_x = numpy.std(x)
    mean_y = numpy.mean(y)
    err_y = numpy.std(y)
    fixed_x = fixed_y = ""

    x2 = filter_outliers(x, mean_x, err_x)
    y2 = filter_outliers(y, mean_y, err_y)
    if x != x2:
        fixed_x = "*" * abs(len(x) - len(x2))
        mean_x = numpy.mean(x2)
        err_x = numpy.std(x2)
    if y != y2:
        fixed_y = "*" * abs(len(y) - len(y2))
        mean_y = numpy.mean(y2)
        err_y = numpy.std(y2)

    u_x = uncertainties.ufloat(mean_x, err_x)
    u_y = uncertainties.ufloat(mean_y, err_y)
    delta = u_x/u_y
    print "%s | %s%s | %s%s | %sx" % (op, u_x, fixed_x, u_y, fixed_y, delta)
