# encoding=UTF-8
import numpy as np
import random
import hashlib
from sklearn.cluster import KMeans

#生成随机向量
from sklearn.metrics.pairwise import cosine_similarity


def sigGen(matrix,k):
    """
    * generate the signature vector
    :param matrix: a ndarray var
    :return a signature vector: a list var
    """
    rownum = matrix.shape[0]

    # the row sequence set

    kmatrix = np.random.normal(size=(k,rownum))

    result = np.dot(kmatrix * matrix)

    # return a list
    return result


def sigMatrixGen(input_matrix, k):
    """
    generate the sig matrix
    :param input_matrix: naarray var
    :param n: the row number of sig matrix which we set
    :return sig matrix: ndarray var
    """

    rownum = input_matrix.shape[0]

    # the row sequence set

    kmatrix = np.random.normal(size=(k, rownum))

    result_pre = np.dot(kmatrix , input_matrix)
    # print result_pre
    result = np.where(np.dot(kmatrix , input_matrix)>0,1,0)
    # print result
    # return a list
    return result

def cosineHash(input_matrix, b, r):
    """
    map the sim vector into same hash bucket
    :param input_matrix:
    :param b: the number of bands
    :param r: the row number of a band
    :return the hash bucket: a dictionary, key is hash value, value is column number
    """

    hashBuckets = {}

    # permute the matrix for n times
    n = b * r

    # generate the sig matrix

    for i in range(b):

        sigMatrix = sigMatrixGen(input_matrix, r)

        for j in range(sigMatrix.shape[1]):
            # generate the hash object, we used md5
            hashObj = hashlib.md5()

            # calculate the hash value
            band = str(sigMatrix[:,j]) + str(i)
            hashObj.update(band.encode())

            # use hash value as bucket tag
            tag = hashObj.hexdigest()

            # update the dictionary
            if tag not in hashBuckets:
                hashBuckets[tag] = [j]
            elif j not in hashBuckets[tag]:
                hashBuckets[tag].append(j)

    # return a dictionary
    return hashBuckets


def nn_search(dataSet, query):
    """
    :param dataSet: 2-dimension array
    :param query: 1-dimension array
    :return: the data columns in data set that are similarity with query
    """

    result = set()



    dataSet.append(query)
    for i in xrange(len(dataSet)):
        print(str(i)+":"+str(dataSet[i]))
    input_matrix = np.array(dataSet).T
    # hashBucket = cosineHash(input_matrix, 20, 12)
    hashBucket = cosineHash(input_matrix, 20, 15)
    print hashBucket
    queryCol = input_matrix.shape[1] - 1

    result1 = set()
    for key in hashBucket:
        if queryCol in hashBucket[key]:
            for i in hashBucket[key]:
                result.add(i)


    # result.remove(queryCol)
    count = 0
    list=[]
    for ele in dataSet:
        dis=cosine_similarity(query.reshape(1,-1),ele.reshape(1,-1))
        print(str(count)+":"+str(dis))
        list.append((dis, count))
        count=count+1
    list.sort(cmp=cmp)
    print list

    for ele in result:
        dis = cosine_similarity(query.reshape(1, -1), dataSet[ele].reshape(1, -1))
        print("similar:"+str(ele) + ":" + str(dis))
        list.append((dis, count))
        count = count + 1
    return result

def cmp(a,b):
    if a[0]>b[0]:
        return -1
    else:
        return 1
if __name__ == '__main__':
    # matrix = np.array([[1,0,1,0],[1,1,0,1],[0,1,0,1],[0,0,0,1],[0,0,0,1],[1,1,1,0],[1,0,1,0]])
    matrix=[]
    for i in xrange(100):
        matrix.append(np.random.normal(size=100))
    print nn_search(matrix,np.random.normal(size=100))
