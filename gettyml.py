from __future__ import division
from __future__ import print_function

from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import desc
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import Row
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from vcgImageAI.comm.sparkBase import *
from vcgImageAI.comm.vcgUtils import *

sparkBase = SparkBase()
spark = sparkBase.createYarnSparkEnv()


def run():
    topFolder = 'hdfs://172.16.241.100:9000/data/mlib_data/getty/'
    gettyImagesMetaFile = '{}allGettyMeta.csv'.format(topFolder)
    # imageId  kwIds  vcgImageId
    print(gettyImagesMetaFile)
    gettyKwIdCountFile = '{}gettyKwIdCount.csv'.format(topFolder)
    fields = [StructField("imageId", StringType()),
              StructField("kwIds", StringType()),
              StructField("vcgImageId", StringType())

              ]
    schema = StructType(fields)
    gettyImagesMeta_df = spark.read.format("csv").option("header", "false").schema(schema).option("delimiter",
                                                                                                  '\t').load(
        gettyImagesMetaFile)
    print('gettyImagesMeta_df: %s' % gettyImagesMeta_df.count())

    gettyImagesMeta_df = gettyImagesMeta_df.filter(gettyImagesMeta_df.kwIds.isNotNull()).rdd.filter(
        lambda row: row.kwIds is not None).toDF()
    print('gettyImagesMeta_df kwIds not null count: %s' % gettyImagesMeta_df.count())

    # compute kwId count, generate kwIdsCount.csv

    def flatMap1(row):
        imageId = row.imageId
        kwIds_ = row.kwIds.split(',')
        rows = []
        for kwId in kwIds_:
            row = Row(imageId=imageId, kwId=kwId)
            rows.append(row)
        return rows

    gettyImagesMeta_df = gettyImagesMeta_df.rdd.filter(lambda row: ((row.kwIds is not None))).flatMap(
        lambda row: flatMap1(row)).toDF().cache()
    gettyImagesMeta_df.show(100, False)
    # print('total imageId-kwId count:%d' % gettyImagesMeta_df.count())

    gettyKwIdCount_df = gettyImagesMeta_df.groupBy("kwId").agg(
        {'*': 'count'}).withColumnRenamed('count(1)', 'count')

    gettyKwIdCount_df = gettyKwIdCount_df.orderBy(desc("count"))
    gettyKwIdCount_df.show(100, False)

    gettyKwIdCount_df.repartition(1).write.format("com.databricks.spark.csv").option("header", "True").option(
        "delimiter",
        '\t').mode(
        "overwrite").save(gettyKwIdCountFile)

    # analysis kwIdsCount.csv  then sort by count desc  select count > topNum kwIds as labels generate related files
    topNum = 300
    gettyKwIdCountFilteredFile = '{}gettyKwIdCountAbove{}.csv'.format(topFolder, 300)
    labelsIndexMappingFile = '{}labelsIndexMappingAbove{}.csv'.format(topFolder, 300)

    gettyKwIdCount_df = spark.read.format("csv").option("header", "true").option("delimiter", '\t').load(
        gettyKwIdCountFile)
    gettyKwIdCount_df.show(10, False)

    gettyKwIdCount_df = gettyKwIdCount_df.filter(gettyKwIdCount_df['count'] > topNum)

    gettyKwIdCount_df = gettyKwIdCount_df.withColumn("index", F.row_number().over(
        Window.orderBy(monotonically_increasing_id())) - 1)
    gettyKwIdCount_df.show(100, False)
    gettyKwIdCount_df.repartition(1).write.format(
        "com.databricks.spark.csv").option("header", "True").option("delimiter",
                                                                    '\t').mode(
        "overwrite").save(gettyKwIdCountFilteredFile)

    gettyKwIdCount_df.select('index', 'kwId').repartition(1).write.format("com.databricks.spark.csv").option("header",
                                                                                                             "False") \
        .option("delimiter", '\t').mode("overwrite").save(
        labelsIndexMappingFile)
    kwIdsSet = set()




    kwIds=gettyKwIdCount_df.select('index', 'kwId').rdd.collect()
    for row in kwIds:
        kwIdsSet.add(row.kwId)
    print('filterd kwIds size: %d'% len(kwIdsSet))

    kwIdsSet_broadcast = spark.sparkContext.broadcast(kwIdsSet)

    gettytopNumImagesOfKwIdFile = '{}kwsTopNumImages.csv'.format(topFolder)
    # kwId,topNumImages

    finalImageKwIdsFile = '{}finalImageKwIds.csv'.format(topFolder)
    gettytopNumImagesOfKwId_df = spark.read.format("csv").option("header", "false").option("delimiter", '\t').load(
        gettytopNumImagesOfKwIdFile).withColumnRenamed('_c0', 'kwId').withColumnRenamed('_c1', 'imageIds')
    gettytopNumImagesOfKwId_df = gettytopNumImagesOfKwId_df.filter(gettytopNumImagesOfKwId_df.imageIds.isNotNull())

    def filterKwIds(row):
        kwId = row.kwId
        if kwId in kwIdsSet_broadcast.value:
            return True
        else:
            return False

    def flatMaps(row):
        kwId = row.kwId
        imageIds = row.imageIds.split(',')
        rows = []
        for imageId in imageIds:
            row = Row(kwId=kwId, imageId=imageId)
            rows.append(row)
        return rows

    gettytopNumImagesOfKwId_df = gettytopNumImagesOfKwId_df.rdd.filter(lambda row: filterKwIds(row)).flatMap(
        lambda row: flatMaps(row)).toDF()

    gettytopNumImagesOfKwId_df = gettytopNumImagesOfKwId_df.groupBy("imageId").agg(
        {'*': 'count'}).withColumnRenamed('count(1)', 'count')
    gettyImagesMeta_df = gettyImagesMeta_df.withColumnRenamed('imageId', 'gettyImageId')
    # gettyImagesMeta_df 过滤 然后合并imageId-kwId to imageId-kwIds (aggregate operation)
    zero_value_2 = None

    def seqFunc_2(accumulator, element):
        if accumulator is None:
            return element
        else:
            element = accumulator + "," + element
            return element

    def combFunc_2(accumulator1, accumulator2):
        if accumulator1 is None:
            return accumulator2
        else:
            accumulator2 = accumulator1 + "," + accumulator2
            return accumulator2

    gettyImagesMeta_df = gettyImagesMeta_df.rdd.filter(lambda row: filterKwIds(row)).map(
        lambda row: (row.gettyImageId, row.kwId)).aggregateByKey(zero_value_2, seqFunc_2, combFunc_2).toDF()
    gettyImagesMeta_df.show(100, False)


    finalImageKwIds_df = gettytopNumImagesOfKwId_df.join(gettyImagesMeta_df,
                                                         gettyImagesMeta_df.gettyImageId == gettytopNumImagesOfKwId_df.imageId,
                                                         how='inner').drop(
        'imageId')
    # add new column url
    gettyImageUrlPrefix = 'https://elephant-data-backup.oss-cn-beijing.aliyuncs.com/elephant-data-backup/gettyimage/'

    def setUrl(gettyImageId):

        return '{}{}.jpg'.format(gettyImageUrlPrefix, gettyImageId)

    setUrlUdf = udf(setUrl, StringType())
    finalImageKwIds_df = finalImageKwIds_df.withColumn('url', setUrlUdf('gettyImageId'))

    finalImageKwIds_df.repartition(1).write.format("com.databricks.spark.csv").option("header", "True").option(
        "delimiter",
        '\t').mode(
        "overwrite").save(finalImageKwIdsFile)

    print('finalImageKwIds count: %d' % finalImageKwIds_df.count())

    # based on generated finalImageKwIds info to generate final tfrecords as train data


if __name__ == '__main__':
    run()
