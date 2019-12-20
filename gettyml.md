#getty image data process 

我们现在有两个基础文件：
- allGettyMeta.csv (50373181 lines)
- gettytopNumImagesOfKwId.csv
allGettyMeta里面保存的是getty所有采集到的图片的gettyId,kwIds（逗号隔开）,以及vcgImageId。
gettytopNumImagesOfKwId文件里面保存的是getty里面采集到的每个关键词top5000的图片id，
格式：kwId,imageIds(逗号隔开)。
上述两个文件位于hdfs cluster上面， 地址分别为：
hdfs://172.16.241.100:9000/data/mlib_data/getty/allGettyMeta.csv
hdfs://172.16.241.100:9000/data/mlib_data/getty/gettytopNumImagesOfKwId.csv

然后我们的需求是
- 基于allGettyMeta文件进行过滤，remove kwIds为空的imageId
- 统计所有的imageId,kwId对的数量 (1385950499)
- 统计每个关键词的图片量
- 获取所有的关键词，它的图片量大于300
- 从gettytopNumImagesOfKwId文件中获取所有的imageId， 这些imageId位于上一步获取的关键词的top5000中，因为可能存在同一个imageId出现在多个
kwId top5000里面， 所有我们需要对所有的imageId进行group ， 得到一个不重复的imageId集合
- 基于上一步获取的imageId集合， 然后再结合allGettyMeta文件（直接inner join），同时考虑到allGettyMeta文件中的kwIds里面可能包含
 一些noisy kwIds， 我们还需要基于第四步获取的kwIds 进行过滤，最终得到文件：finalImageKwIds， 格式：
gettyImageId,kwIds,url
- 最后一步是基于上一步产生的文件生成模型训练数据，格式是tfrecords的， 格式是：imageId,kwIds,imageBytes


