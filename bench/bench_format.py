#!/usr/bin/python

import uncertainties
import numpy
import sys

def filter_outliers(numbers, mean, std):
    return filter(lambda x: abs(x - mean) < 2 * std, numbers)

op_str = {
    'create_files' : 'Create 100 files',
    'create_files_parallel' : 'Create 100 files (parallel)',
    'rm_files' : 'Unlink 100 files',
    'rm_files_parallel' : 'Unlink 100 files (parallel)',
    'ls_files' : 'ls with 1000 files',
    'find_files' : "`find' with 1000 dirs/files",
    'write_md5' : 'Write 1GB',
    'read_first_byte' : 'Time to 1st byte',
    'read_md5' : 'Read 1GB',
}

outputOrder = [
    'create_files',
    'create_files_parallel',
    'rm_files',
    'rm_files_parallel',
    'ls_files',
    'find_files',
    'write_md5',
    'read_md5',
    'read_first_byte',
]

f = sys.argv[1]
data = open(f).readlines()
#print 'operation | goofys |  s3fs  | speedup'
#print '----------| ------ | ------ | -------'

table = [{}, {}, {}]
has_data = {}

print '#operation,time'
for l in data:
    dataset = l.strip().split('\t')
    for d in range(0, len(dataset)):
        op, num = dataset[d].split(' ')
        if not op in table[d]:
            table[d][op] = []
        table[d][op] += [float(num)]
        has_data[op] = True


for c in outputOrder:
    if c in has_data:
        print op_str[c],
        for d in table:
            mean = numpy.mean(d[c])
            err = numpy.std(d[c])
            x = filter_outliers(d[c], mean, err)
            print "\t%s\t%s\t%s" % (numpy.mean(x), numpy.min(x), numpy.max(x)),
        print

    # op = op_str[nums[0]]

    # for i in range(1, len(nums)):
        
    # x = map(lambda x: float(x), nums[1].strip().split(' '))
    # y = map(lambda x: float(x), nums[2].strip().split(' '))
    # mean_x = numpy.mean(x)
    # err_x = numpy.std(x)
    # mean_y = numpy.mean(y)
    # err_y = numpy.std(y)
    # fixed_x = fixed_y = ""

    # x2 = filter_outliers(x, mean_x, err_x)
    # y2 = filter_outliers(y, mean_y, err_y)
    # if x != x2:
    #     fixed_x = "*" * abs(len(x) - len(x2))
    #     mean_x = numpy.mean(x2)
    #     err_x = numpy.std(x2)
    # if y != y2:
    #     fixed_y = "*" * abs(len(y) - len(y2))
    #     mean_y = numpy.mean(y2)
    #     err_y = numpy.std(y2)

    # print "%s, %s, %s, %s", op, mean_x, mean_x - err_x, mean_x + err_x
    # # u_x = uncertainties.ufloat(mean_x, err_x)
    # # u_y = uncertainties.ufloat(mean_y, err_y)
    # # delta = u_y/u_x
    # # print "%s | %s%s | %s%s | %sx" % (op, u_x, fixed_x, u_y, fixed_y, delta)
