import sys

def generate_big_random_bin_file(filename,size):
    """
    generate big binary file with the specified size in bytes
    :param filename: the filename
    :param size: the size in bytes
    :return:void
    """
    import os
    fs = size*1048576
    with open('%s'%filename, 'wb') as fout:
        fout.write(os.urandom(fs)) #1

    print 'big random binary file with size %f generated ok'%fs
    pass

fn = sys.argv[1]
size = int(sys.argv[2])
generate_big_random_bin_file(fn,size)
