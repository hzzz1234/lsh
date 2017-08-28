#encoding=utf-8
'''
created on 2017-08-07
@author: zhen.huaz
@usage: hash for jaccard
'''
import random

import numpy as np
import sys

'''
    @parameter:matrix hashtype hashnum
'''
# //解析参数                                                                                                                                                          int minClusterSize = Integer.valueOf(getOption(MinhashOptionCreator.MIN_CLUSTER_SIZE));//每个类中的最小point个数，默认10
#     int minVectorSize = Integer.valueOf(getOption(MinhashOptionCreator.MIN_VECTOR_SIZE));//最小向量大小，默认5
#     String hashType = getOption(MinhashOptionCreator.HASH_TYPE);//Hash类型，可选: (linear, polynomial, murmur)，默认murmur
#     int numHashFunctions = Integer.valueOf(getOption(MinhashOptionCreator.NUM_HASH_FUNCTIONS));//Hash函数的个数，默认10
#     int keyGroups = Integer.valueOf(getOption(MinhashOptionCreator.KEY_GROUPS));//key的组数，默认2
#     int numReduceTasks = Integer.parseInt(getOption(MinhashOptionCreator.NUM_REDUCERS));//reduce个数，默认2
#     boolean debugOutput = hasOption(MinhashOptionCreator.DEBUG_OUTPUT);//debug的输出路径
#

LINER = 'linerhash'
POLYNOMIAL = 'polynomial'
MURMUR = 'murmur'

# liner hash
# def linerhash(bytes[])
#
# def polynomialHash:
#
# def murmurhash:


def hash1(x,n):
    return (x+1)%n
def hash2(x,n):
    return (3*x+1)%n

def sigGen(matrix):
    npMatrix = np.array(matrix)
    f = [hash1,hash2]
    n,m = npMatrix.shape;
    l1 = []
    for fh in f:
        l2 = []
        for m1 in range(m):

            min = sys.maxint
            for n1 in range(n):
                ele = npMatrix[n1,m1]
                if ele == 0:
                    continue
                else:
                    h = fh(n1,n)
                    if min > h:
                        min = h
            l2.append(min)
        l1.append(l2)
    return l1







if __name__=='__main__':
    a = {'nike', 'running', 'shoe'}
    b = {'nike', 'black', 'running', 'shoe'}
    c = {'nike', 'blue', 'jacket'}
    matrix = [[1, 1, 1], [1, 1, 0], [1, 1, 0], [0, 1, 0], [0, 0, 1], [0, 0, 1]]
    print sigGen(matrix)