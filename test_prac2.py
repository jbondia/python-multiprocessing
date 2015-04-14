from __future__ import division
from py_ecc import file_ecc
from random import shuffle

import os
import multiprocessing

MAX_PROCESS =  8
num_recovery_blocks = [2, 4, 7]

def test_distributed(n, k, filename):
    files = file_ecc.EncodeFile(filename, 'init', n, k)
    #print files
    return files


def test_recover_distributed(files):

    out_name = "init_out.py"

    try:
        file_ecc.DecodeFiles(files, out_name)
        f = open(out_name, "r")
        data = f.read()
        f2 = open("__init__.py","r")
        original_data = f2.read()

        if original_data == data:
            print 'Reconstructed file with ', len(files), 'blocks'
        else:
            print 'Could not recover file with ', len(files), 'blocks'

        f.close()
        f2.close
        os.remove(out_name)
    except:
        print 'Could not recover file with ', len(files), 'blocks'


def test_recover_file(l_files):
    for i in range (1,len(l_files) + 1):
        l = l_files[:i]
        shuffle(l)
        test_recover_distributed(l)

def remove_partial_files(files):
    for f in files:
        os.remove(f)

def save_block(queue, filename):
    queue.put(filename)


def test_exercise_1():
    print '\n#####\tExercise 1\t#####'
    for blocks in num_recovery_blocks:
        print '###\tERASURE CODES: ', blocks, '\t###'
        print 'Storage cost ratio = ', MAX_PROCESS / blocks
        print 'Additional storage cost ratio = ',  MAX_PROCESS / blocks - 1
        print 'Correcting bits ratio = ', MAX_PROCESS / blocks - 1
        l_files = test_distributed(MAX_PROCESS, blocks, "__init__.py")
        shuffle(l_files)

        sent_blocks = [0] * MAX_PROCESS
        received_blocks = [0] * MAX_PROCESS
        collaboration_ratio = []

        queue = multiprocessing.Queue()
        p = []
        for i in range(MAX_PROCESS):
            received_blocks[i] += 1
            p.append(multiprocessing.Process(target=save_block, args=(queue,l_files[i])))
            p[i].start()
        for i in range(MAX_PROCESS):
            p[i].join()

        i = 0
        recovery_list = []
        while not queue.empty() and i < blocks:
            recovery_list.append(queue.get())
            sent_blocks[i] += 1
            i += 1

        for i in range(MAX_PROCESS):
            collaboration_ratio.append(sent_blocks[i] / received_blocks[i])
            print 'Collaboration ratio P', i, ' = ', collaboration_ratio[i]
        print 'Total collaboration ratio = ', sum(collaboration_ratio)
        test_recover_distributed(recovery_list)
        print''

        queue.close()
        queue.join_thread()
        remove_partial_files(l_files)
    print '###################################################\n'


def test_exercise_2():
    print '\n#####\tExercise 2\t#####'
    num_failures = range(1,6)

    for blocks in num_recovery_blocks:
        print '###\tERASURE CODES: ', blocks, '\t###'
        l_files = test_distributed(MAX_PROCESS, blocks, "__init__.py")
        shuffle(l_files)

        for failure in num_failures:
            print 'Number of failures => ', failure
            queue = multiprocessing.Queue()
            p = []
            for i in range(MAX_PROCESS - failure):
                p.append(multiprocessing.Process(target=save_block, args=(queue,l_files[i])))
                p[i].start()
            for i in range(MAX_PROCESS - failure):
                p[i].join()

            recovery_list = []
            while not queue.empty() and len(recovery_list) < blocks:
                recovery_list.append(queue.get())
            test_recover_distributed(recovery_list)

            queue.close()
            queue.join_thread()

        remove_partial_files(l_files)
        print ''


def test_shuffled_files():
    print '#####\tSeveral shuffled files. File recovery demonstration\t#####'
    for blocks in num_recovery_blocks:
        print '###\tERASURE CODES: ', blocks, '\t###'
        print 'Storage cost ratio = ', MAX_PROCESS / blocks
        print 'Additional storage cost ratio = ',  MAX_PROCESS / blocks - 1
        print 'Correcting bits ratio = ', MAX_PROCESS / blocks - 1
        l_files = test_distributed(MAX_PROCESS, blocks, "__init__.py")
        test_recover_file(l_files)
        remove_partial_files(l_files)
        print '###################################################\n'

if __name__ == "__main__":
    #test_shuffled_files()
    test_exercise_1()
    test_exercise_2()
