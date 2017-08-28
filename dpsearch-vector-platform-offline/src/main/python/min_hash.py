import numpy as np
import random
import hashlib


def sigGen(matrix):
    """
    * generate the signature vector
    :param matrix: a ndarray var
    :return a signature vector: a list var
    """

    # the row sequence set
    seqSet = [i for i in range(matrix.shape[0])]

    # initialize the sig vector as [-1, -1, ..., -1]
    result = [-1 for i in range(matrix.shape[1])]

    count = 0

    while len(seqSet) > 0:

        # choose a row of matrix randomly
        randomSeq = random.choice(seqSet)

        for i in range(matrix.shape[1]):

            if matrix[randomSeq][i] != 0 and result[i] == -1:
                result[i] = randomSeq
                count += 1
        if count == matrix.shape[1]:
            break

        seqSet.remove(randomSeq)

    # return a list
    return result


def sigMatrixGen(input_matrix, n):
    """
    generate the sig matrix
    :param input_matrix: naarray var
    :param n: the row number of sig matrix which we set
    :return sig matrix: ndarray var
    """

    result = []

    for i in range(n):
        sig = sigGen(input_matrix)
        result.append(sig)

    # return a ndarray
    return np.array(result)


def minHash(input_matrix, b, r):
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
    sigMatrix = sigMatrixGen(input_matrix, n)

    # begin and end of band row
    begin, end = 0, r

    # count the number of band level
    count = 0

    while end <= sigMatrix.shape[0]:

        count += 1

        # traverse the column of sig matrix
        for colNum in range(sigMatrix.shape[1]):

            # generate the hash object, we used md5
            hashObj = hashlib.md5()

            # calculate the hash value
            band = str(sigMatrix[begin: begin + r, colNum]) + str(count)
            hashObj.update(band.encode())

            # use hash value as bucket tag
            tag = hashObj.hexdigest()

            # update the dictionary
            if tag not in hashBuckets:
                hashBuckets[tag] = [colNum]
            elif colNum not in hashBuckets[tag]:
                hashBuckets[tag].append(colNum)
        begin += r
        end += r

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
    count= 0
    # for ele in dataSet:
    #     print(str()+":"+str(ele))
    #     count = count+1
    input_matrix = np.array(dataSet).T
    hashBucket = minHash(input_matrix, 20, 8)
    print hashBucket
    queryCol = input_matrix.shape[1] - 1

    for key in hashBucket:
        if queryCol in hashBucket[key]:
            for i in hashBucket[key]:
                result.add(i)

    result.remove(queryCol)
    count=0
    list=[]
    for ele in dataSet:
        dis=haming_distance(query.reshape(1,-1),ele[0:1000].reshape(1,-1))
        list.append((dis,count))
        print(str(count)+":"+str(dis))
        count=count+1

    list.sort(cmp=cmp)
    print list
    return result

def cmp(a,b):
    if a[0]>b[0]:
        return -1
    else:
        return 1

def haming_distance(a,b):

    return float(np.sum(a & b))/np.sum(a|b)


if __name__ == '__main__':
    # matrix = np.array([[1,0,1,0],[1,1,0,1],[0,1,0,1],[0,0,0,1],[0,0,0,1],[1,1,1,0],[1,0,1,0]])
    matrix=[]
    for i in xrange(100):
        matrix.append(np.random.randint(2,size=1000))

    print nn_search(matrix,np.random.randint(2,size=1000))