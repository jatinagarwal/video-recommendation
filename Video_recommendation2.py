
# coding: utf-8

# In[14]:
from pyspark import SparkContext
sc = SparkContext("local", "Video-Recommendation")
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("video recommendation").config("spark.some.config.option", "some-value").getOrCreate()


# In[15]:

type(sc)


# In[16]:

df = spark.read.json("file:///home/jatin/Desktop/Videos_Data_150819+6477.json")


# In[17]:

type(df)


# In[18]:

df.printSchema()


# In[19]:

relevantFields = df.select("id","name","description","tags").rdd


# In[20]:

" ".join(map(unicode,relevantFields.first().asDict().values())).replace("0_kirvzs3g", "")


# In[21]:

type(relevantFields)


# In[22]:

def formatAsDict(row):
    key = str(row['id'])
    value = " ".join(map(unicode,row.asDict().values())).replace(key, "")
    return (key,value)
    
# myRdd = relevantFields.map()


# In[23]:

formatAsDict(relevantFields.first())


# In[24]:

nameFromCsv = relevantFields.repartition(8).map(formatAsDict)


# In[25]:

# nameFromCsv.saveAsTextFile("videoData")


# In[26]:

type(nameFromCsv)


# In[27]:

# inputPath="/home/jatin/Documents/spark-notebook-0.6.2-scala-2.11.7-spark-1.6.0-hadoop-2.2.0-with-hive-with-parquet/notebooks/video-recommendation/videoMetaDataInCsvFormat"


# In[28]:

englishStopWords = ["a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"]


# In[29]:

# #Reading file containing video meta data to make initial RDD
# readFile = sc.textFile(inputPath).cache()
# # readFile.count()


# In[30]:

# readFile.take(5)


# In[31]:

# def formatInput(item):
#      name = item.split(",")[0]
#      return (name,item.replace(name+",",""))

# nameFromCsv = readFile.map(formatInput)
# #                                name = item.split(",")(0)
# #                               (name,item.replace(name+",",""))
# #                               }


# In[32]:

# nameFromCsv.take(2)


# In[33]:

reserveWords = sc.broadcast(englishStopWords)


# In[34]:

import re
def bagOfWords(item):
    regexSpecialChars = """[^a-zA-Z0-9\s]"""
    noSymbolsLine = re.sub(regexSpecialChars, " ",item)
    stopWords = reserveWords.value
    listOfWords = noSymbolsLine.split()
    nonEmptyWords = filter(lambda x :x != "",listOfWords)
    removeReserveWords = []
    for word in nonEmptyWords:
        if word not in stopWords:
            removeReserveWords.append(word.lower())
    return removeReserveWords


# In[35]:

bow = bagOfWords("'Edge of Alaska': Keeping a Frontier 1232 Town  @ #Remote,All it takes is () *%* one big attraction to change everything. Will McCarthy be next?,\"reality tv series, edge of alaska, mccarthy alaska, frontier town, off grid living, takeetna alaska\"")
print bow


# In[36]:

cleandData = nameFromCsv.mapValues(bagOfWords).cache()
# print cleandData


# In[37]:

# Cross Checking count of data after cleaning 
numDocs = cleandData.count()


# In[38]:

print numDocs


# In[39]:

# readFile.unpersist()
from collections import Counter
def calculateTermFreq(item):
    return Counter(item).items()


# In[40]:

it = calculateTermFreq(bow)


# In[41]:

print it


# In[42]:

termDocumentFrequencies = cleandData.mapValues(calculateTermFreq)


# In[43]:

sampleItem = termDocumentFrequencies.take(1)[0][1]
print sampleItem


# In[44]:

idDocs = termDocumentFrequencies.map(lambda x: x[0]).zipWithUniqueId().collectAsMap()#.toMap


# In[45]:

type(idDocs)


# In[46]:

# idDocs


# In[47]:

docIds = dict((v,k) for k,v in idDocs.iteritems())
# docIds.take(2)


# In[48]:

docIds[0]


# In[49]:

docVect = termDocumentFrequencies.flatMapValues(lambda x:x).values().reduceByKey(lambda x,y:x + y).cache()


# In[50]:

cleandData.unpersist()


# In[51]:

docVect.count()


# In[52]:

docVect.take(10)[2][0]


# In[53]:

vocabRdd = docVect.filter(lambda item: item[1] > 5 and len(item[0]) > 3).cache()
corpusVocab = vocabRdd.collect()


# In[54]:

numOfFeatures = vocabRdd.count()
vocabRdd.unpersist()
print numOfFeatures


# In[55]:

# FUNCTION TO COMPUTE INVERSE DCOUMENT FREQUENCIES
from math import log1p
def inverseDocumentFrequencies(docFreqs, numDocs):
    return map( lambda tup : (tup[0], log1p(float(numDocs) / tup[1])), docFreqs)


# In[56]:

#Computing inverse document frequencies 'idfs' from document frequencies
idfs = inverseDocumentFrequencies(corpusVocab, numDocs)


# In[57]:

idfsMap = dict(idfs)


# In[58]:

# /* Broadcasting 'idfs' across nodes of cluster*/
bidfs = sc.broadcast(idfsMap)


# In[59]:

# /* Collecting all the terms after filtering (terms, df) pairs*/
termsIds = dict(enumerate(idfsMap.keys()))#.zipWithIndex()
# print termsIds/


# In[60]:

type(termsIds)


# In[61]:

vocabulary = dict((v,k) for k,v in termsIds.iteritems())
termList = sc.broadcast(vocabulary)


# In[62]:

docVectWithId =  docVect.zipWithIndex().map(lambda x:(x[1],x[0])).cache()
# // val pairs = docVectWithId.values.keys
docVect.unpersist()


# In[63]:

import numpy
from pyspark.mllib.linalg import *
from pyspark import StorageLevel
from pyspark.mllib.linalg.distributed import RowMatrix
def createVector(item):
    itemDict = dict(item)
    allIdentifiers = termList.value#/* Locally obtaining broadcasted  values */
    termInThisDocument = itemDict.keys()#/* Obtaining all terms from this document*/
    sizeOfVector = len(allIdentifiers)#/* Computing number of terms(identifiers) across all the documents*/
    tfidfMap = dict()#/* Computing a map of (identifier, tfidf) pairs from term-document
#            (identifier, count) pairs and document-frequency (identifier, idfs) pair */
    for term in termInThisDocument:
        if term in allIdentifiers:
            tfidfMap[allIdentifiers[term]]=itemDict[term]
    return Vectors.sparse(numOfFeatures,tfidfMap)
#     return tfidfMap
tfidfVector = termDocumentFrequencies.mapValues(createVector)
# import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors, Matrices, Vector}
# import org.apache.spark.storage.StorageLevel

# /* Constructing vector for metData of every video */

# val tfidfVector: RDD[(String, Vector)] = termDocumentFrequencies.mapValues{termFreqPair =>
#       val allIdentifiers = termList.value/* Locally obtaining broadcasted  values */
#       //val docTotalTerms: Double = termFreqPair.values.sum + 0.0
#       val termInThisDocument: List[String] = termFreqPair.keySet.toList/* Obtaining all terms from this document*/
#       val sizeOfVector: Int = allIdentifiers.size/* Computing number of terms(identifiers) across all the documents*/
#       var tfidfMap:Map[Int,Double] = Map()/* Computing a map of (identifier, tfidf) pairs from term-document
#            (identifier, count) pairs and document-frequency (identifier, idfs) pair */
#       for(term <- termInThisDocument if allIdentifiers.contains(term)) {
#         tfidfMap += (allIdentifiers(term) -> termFreqPair(term)) /* TFIDF computation */
#       }
#       val tfidfSeq: Seq[(Int, Double)] = tfidfMap.toSeq/* Converting 'tfidfMap' map to a sequence */
#       Vectors.sparse(sizeOfVector, tfidfSeq) /*Obtaining sparse vector from 'tfidfSeq' sequence and 'sizeOfVector'*/
# }
# tfidfVector.persist(StorageLevel.MEMORY_AND_DISK)


# In[64]:

type(tfidfVector)


# In[65]:

createVector(sampleItem)


# In[66]:

createVector(sampleItem)


# In[67]:

tfidfVector.persist(StorageLevel.MEMORY_AND_DISK)
tfidfVector.count()
docVect.unpersist()


# In[68]:

#/* Constructing row matrix for terms and metaData of each video */
mat = RowMatrix(tfidfVector.values())
m = mat.numRows()# /* Number of rows in a matrix */
n= mat.numCols()# /* Number of columns in a matrix */
#/* Computing svd from the 'mat' to obtain matrices*/
# svd = mat.computeSVD(30, computeU=true)


# In[69]:

type(mat)


# In[70]:

# http://stackoverflow.com/questions/33428589/pyspark-and-pca-how-can-i-extract-the-eigenvectors-of-this-pca-how-can-i-calcu/33500704#33500704
from pyspark.mllib.common import callMLlibFunc, JavaModelWrapper
from pyspark.mllib.linalg.distributed import RowMatrix


# In[71]:

class SVD(JavaModelWrapper):
    """Wrapper around the SVD scala case class"""
    @property
    def U(self):
        """ Returns a RowMatrix whose columns are the left singular vectors of the SVD if computeU was set to be True."""
        u = self.call("U")
        if u is not None:
            return RowMatrix(u)

    @property
    def s(self):
        """Returns a DenseVector with singular values in descending order."""
        return self.call("s")

    @property
    def V(self):
        """ Returns a DenseMatrix whose columns are the right singular vectors of the SVD."""
        return self.call("V")


# In[72]:

def computeSVD(row_matrix, k, computeU=False, rCond=1e-9):
    """
    Computes the singular value decomposition of the RowMatrix.
    The given row matrix A of dimension (m X n) is decomposed into U * s * V'T where
    * s: DenseVector consisting of square root of the eigenvalues (singular values) in descending order.
    * U: (m X k) (left singular vectors) is a RowMatrix whose columns are the eigenvectors of (A X A')
    * v: (n X k) (right singular vectors) is a Matrix whose columns are the eigenvectors of (A' X A)
    :param k: number of singular values to keep. We might return less than k if there are numerically zero singular values.
    :param computeU: Whether of not to compute U. If set to be True, then U is computed by A * V * sigma^-1
    :param rCond: the reciprocal condition number. All singular values smaller than rCond * sigma(0) are treated as zero, where sigma(0) is the largest singular value.
    :returns: SVD object
    """
    java_model = row_matrix._java_matrix_wrapper.call("computeSVD", int(k), computeU, float(rCond))
    return SVD(java_model)


# In[73]:

type(mat._java_matrix_wrapper)


# In[74]:

from pyspark.ml.feature import *
from pyspark.mllib.linalg import Vectors


# In[75]:

data = [(Vectors.dense([0.0, 1.0, 0.0, 7.0, 0.0]),), (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),), (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
# df = sqlContext.createDataFrame(data,["features"])


# In[76]:

svd = computeSVD(mat,30,True)


# In[77]:

type(svd)


# In[78]:

U = svd.U


# In[79]:

type(U)


# In[80]:

docSpaceRDD = U.rows.map(lambda x: x.toArray()[0]).zipWithUniqueId().sortByKey(False).take(5)


# In[81]:

type(docSpaceRDD)


# In[82]:

docSpace = docSpaceRDD#.collect()


# In[83]:

type(docSpace)


# In[84]:

docSpaceRDD#.count()


# In[85]:

type(docSpace[0])


# In[86]:

docSpace[0]


# In[87]:

type(U)


# In[88]:

V = svd.V


# In[89]:

type(V)


# In[90]:

varr = V.toArray()
type(varr)


# In[91]:

V.numRows


# In[92]:

varr.shape


# In[93]:

sigma = svd.s


# In[94]:

print sigma


# In[95]:

type(sigma)


# In[96]:

print sigma


# In[97]:

def topTermsInTopConcepts(svd,numConcepts,numTerms,termIds):
    v = svd.V #Matrix representing term space
    topTerms = []
    arr = v.toArray()
    rows = v.numRows
    for i in xrange(numConcepts):
        termWeights = arr[:,i]#.zipWithIndex()
        termList = list(enumerate(termWeights))
        sortedArr = sorted(termList,key=lambda x: x[1],reverse=True)
        topTerms.append(map(lambda x: (termIds[x[0]],x[1]),sortedArr[:numTerms]))              
    return topTerms


# In[98]:

def findVidId(item):
    docID = ""
    idd = item[1]
    if docIds.has_key(idd):
        docID = docIds[idd]
    else:
        docID = None
    return (docID,item[0])
        
def topDocsInTopConcepts(svd,numConcepts,numDocs,docIds):
    u = svd.U
    topDocs = []
    for i in xrange(numConcepts):
        docWeights =u.rows.map(lambda x: x.toArray()[i]).zipWithUniqueId().sortByKey(False).take(numDocs)
        topDocs.append(map(findVidId,docWeights))
    return topDocs


# In[99]:

#/* Extracts top terms from top most concepts */
topConceptTerms = topTermsInTopConcepts(svd, 15, 15, termsIds)
#/* Extracts top documents from top most concepts */
topConceptDocs = topDocsInTopConcepts(svd, 15, 5, docIds)


# In[100]:

def zipper(x,y):
    k = list(str(a[0]) for a in x)
    v = list(str(a[0]) for a in y)
    return (k,v)
concepts = map(zipper,topConceptTerms, topConceptDocs)


# In[224]:

from pyspark.sql import Row
videoDataMap = nameFromCsv.cache()
videosMetaData = videoDataMap.map(lambda x: Row(Id=x[0],Info=x[1]))
schemaVideos = spark.createDataFrame(videosMetaData)
schemaVideos.createOrReplaceTempView("Videos")
vidID = "0_2umw3t4n"
vidInQuotes = "'%s'" % vidID
query = "SELECT Info FROM Videos WHERE Id = " +vidInQuotes
print query
print "SELECT Info FROM Videos WHERE Id = '0_2umw3t4n'"
info = spark.sql(query)
print info.collect()[0]['Info']

def docMeaning(vidId):
    if vidId != 'None':
        vidInQuotes = "'%s'" % vidId
        query = "SELECT Info FROM Videos WHERE Id = " +vidInQuotes
        info = spark.sql(query).collect()
#             print type(info)
        if(len(info) == 1):
            print vidId
            a = info[0]['Info']
	    print a.encode('ascii', 'ignore')		
            print
            
# for k,v in concepts:
#     print "Concept Terms:"
#     print k
#     print "Concepts Documents:"
#     print v
#     for a in v:
#         docMeaning(a)
#     print
#     print


# In[225]:

docMeaning("0_2umw3t4n")


# In[229]:

# def multiplyByDiagonalMatrix(mat: RowMatrix, diag: Vector): RowMatrix = {
#     val sArr: Array[Double] = diag.toArray
#     new RowMatrix(mat.rows.map(vec => {
#       val vecArr: Array[Double] = vec.toArray
#       val newArr: Array[Double] = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
#       Vectors.dense(newArr)
#     }))
# }

# def rowsNormalized(mat: RowMatrix): RowMatrix = {
#     new RowMatrix(mat.rows.map(vec => {
#       val length: Double = math.sqrt(vec.toArray.map(x => x * x).sum)
#       Vectors.dense(vec.toArray.map(_ / length))
#     }))
# }
import math
sArr = svd.s.toArray()
def multiply(vector):
    vecArr = vector.toArray()
    newArr = []
    for i in xrange(vecArr.size):
        newArr.append(vecArr[i]*sArr[i])
    return newArr

def normalizeRows(vector):
    vecArr = vector.toArray()
    length = math.sqrt(sum(map(lambda x: x*x,vecArr)))
    output = map(lambda x: x/length,vecArr)
    return output
            
def multiplyByDiagonalMatrix(mat,diag):
#     sArr = diag.toArray()
    return RowMatrix(mat.rows.map(multiply))
    
def rowsNormalized(mat):
    return RowMatrix(mat.rows.map(normalizeRows))


# In[230]:

US = multiplyByDiagonalMatrix(svd.U, svd.s)
normalizedUS = rowsNormalized(US)


# In[240]:

docRowArr = []
def row(mat,idd):
    return mat.rows.zipWithUniqueId().map(lambda x: (x[1],x[0])).lookup(idd)[0].toArray()
    
def matMultiply(row):
    vecArr = row.toArray()
    newArr = []
    summ = 0
    for i in xrange(vecArr.size):
        summ = summ + vecArr[i]*docRowArr[i]
    return summ    

def topDocsForDoc(normalizedUS, docId, docIds, numTopDocs):
    global docRowArr
    docRowArr = row(normalizedUS, docId)
    docRowVec = Matrices.dense(len(docRowArr), 1, docRowArr)
    docScores = normalizedUS.rows.zipWithUniqueId().map(lambda x: (x[1],x[0])).mapValues(matMultiply).map(lambda x: (x[1],x[0])).sortByKey(False)
    similarDocs = map(lambda x: docIds[x[1]],docScores.take(numTopDocs))
    map(lambda x: docMeaning(x),similarDocs)
    
def printTopDocsForDoc(normalizedUS, doc, idDocs, docIds, numTopDocs):
    if idDocs.has_key(doc):
        docID = idDocs[doc]
        output = topDocsForDoc(normalizedUS, docID, docIds, numTopDocs)
    else:
        print "Document not found. Enter another document"


# In[241]:

printTopDocsForDoc(normalizedUS,'0_kirvzs3g', idDocs, docIds, 10)


# In[245]:

# sArr = svd.s.toArray()


# In[246]:

# type(US)


# In[247]:

# type(normalizedUS)


# In[248]:

# US.rows.first()


# In[250]:

# normalizedUS.rows.first()


# In[251]:

# idd = idDocs['0_kirvzs3g']


# In[252]:

# print idd


# In[253]:

# rowMat = normalizedUS.rows.zipWithUniqueId().map(lambda x: (x[1],x[0])).lookup(idd)[0].toArray()


# In[254]:

# type(rowMat)


# In[255]:

# rowMat.first()


# In[256]:

# docRowArr = row(normalizedUS,0)


# In[257]:

# type(docRowArr)


# In[258]:

# len(docRowArr)


# In[259]:

# docRowVect = Matrices.dense(len(docRowArr), 1, docRowArr)


# In[260]:

# type(docRowVect)


# In[261]:

# docRowVect


# In[263]:

# type(normalizedUS)


# In[262]:

# def matMultiply(row):
#     vecArr = row.toArray()
#     newArr = []
#     summ = 0
#     for i in xrange(vecArr.size):
#         summ = summ + vecArr[i]*docRowArr[i]
#     return summ    


# In[202]:

#docScores = normalizedUS.rows.zipWithUniqueId().map(lambda x: (x[1],x[0])).mapValues(matMultiply).map(lambda x: (x[1],x[0])).sortByKey(False)


# In[264]:

# type(docScores)


# In[265]:

# docScores.count()


# In[266]:

# docScores.take(10)


# In[244]:

# similarDocs = map(lambda x: docIds[x[1]],docScores.take(10))


# In[242]:

# map(lambda x: docMeaning(x),similarDocs)


# In[ ]:



