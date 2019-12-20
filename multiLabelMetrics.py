from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os.path
import random
import re
from operator import add
import csv

import pyspark
import pyspark.sql.functions as f
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import Row
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.mllib.evaluation import MultilabelMetrics

from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import cv2


import mysql.connector
import requests


from vcgImageAI.comm.sparkBase import *
from vcgImageAI.comm.vcgUtils import *

sparkBase = SparkBase()
spark = sparkBase.createYarnSparkEnv()


"""
从mongo数据表：evaluations计算recall, Precision 以及F-measure, 还有per label accuracy
一个globalIdentity 包含多个modelName（表示模型训练的不同steps），
一个globalIdentity 表示一组唯一的模型以及参数

"""
def computeMultiLabelMetrics(pickedNum=5,topNum=None,globalIdentity=None,batchNum=None,spark=None, excludeLabels=[]):





    pipeline = "[{'$match':{'$and':[{'batchNum':" + str(
        batchNum) + "},{'globalIdentity':'" + globalIdentity + "'}]}},{'$project': {'scores': 1,'labelId':1}}]"
    print(pipeline)

    df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option('uri', 'mongodb://zhaoyufei:vcgjasstion@172.16.241.100/') \
        .option('database', 'vcg') \
        .option('collection', 'evaluations') \
        .option("pipeline", pipeline).load()
    # .persist(storageLevel=StorageLevel.DISK_ONLY)
    df.printSchema()
    df.show(100, False)

    #.persist(storageLevel=StorageLevel.DISK_ONLY)
    df.printSchema()

    # print(df.count())





    excludeLabels_braodcast = spark.sparkContext.broadcast(excludeLabels)




    """
    计算top 20,50, 100,200 的recall以及precision, 
    predictions: labelIndex1,....,  
    labels: labelIndex1,......
    imageId
    """
    def flatMapToLabelId(row):
        labelId = row.labelId
        scores = row.scores
        labelIds = labelId
        # imageId=row.imageId

        labelIds_=[]
        for labelIdV in labelIds:
            if str(labelIdV) in excludeLabels_braodcast.value:

                continue
            labelIds_.append(labelIdV)





        sorted_by_value = sorted(scores.items(), key=lambda kv: -kv[1])

        sorted_by_value_ = []
        for tuple in sorted_by_value:
            if tuple[0] in excludeLabels_braodcast.value:
                continue
            else:
                sorted_by_value_.append(tuple)
        # if topNum is None:
        #     topNum = len(labelIds_) * pickedNum
        # else:
        #     topNum=100





        predictions=[]
        for tupe in sorted_by_value_[0:topNum]:
            predictions.append(float(tupe[0]))
        labelIds=list(map(lambda x: float(x), labelIds_))
        if len(predictions)==0 or len(labelIds) == 0:
            return None
        return (predictions,labelIds)






    rdd = df.rdd.map(lambda row: flatMapToLabelId(row)).filter(lambda tuple:tuple !=None)
    rdd2 = rdd.map(lambda tuples: Row(topNum=len(tuples[0]))).toDF()
    rdd2.show(100, False)
    # print(rdd2.groupBy().sum().collect())
    # Instantiate metrics object
    metrics = MultilabelMetrics(rdd)

    # Summary stats
    print("batchNum = %s" % batchNum)
    print("Recall = %s" % metrics.recall())
    print("Precision = %s" % metrics.precision())
    print("F1 measure = %s" % metrics.f1Measure())
    print("Accuracy = %s" % metrics.accuracy)


if __name__ == '__main__':
    computeMultiLabelMetrics(topNum=100, globalIdentity='pt_gettyml_labelCountAbove300', batchNum=320000,
                             spark=spark)
