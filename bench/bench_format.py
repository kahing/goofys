#!/usr/bin/python

import uncertainties
import numpy
import sys

f = sys.argv[1]
data = open(f).readlines()
print 'operation |  s3fs  | goofys | speedup'
print '----------| ------ | ------ | -------'
for l in data:
    nums = l.strip().split('\t')
    op = nums[0]
    x = map(lambda x: float(x), nums[1].strip().split(' '))
    y = map(lambda x: float(x), nums[2].strip().split(' '))
    mean_x = numpy.mean(x)
    err_x = numpy.std(x)
    mean_y = numpy.mean(y)
    err_y = numpy.std(y)
    u_x = uncertainties.ufloat(mean_x, err_x)
    u_y = uncertainties.ufloat(mean_y, err_y)
    delta = u_x/u_y
    print "%s | %s | %s | %s" % (op, u_x, u_y, delta)
