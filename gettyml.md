#getty data process demo

我们现在有两个基础文件：
- allGettyMeta.csv
- gettytopNumImagesOfKwId.csv
allGettyMeta里面保存的是getty所有采集到的图片的gettyId,kwIds（逗号隔开）,以及vcgImageId。
gettytopNumImagesOfKwId文件里面保存的是getty里面采集到的每个关键词top5000的图片id，
格式：kwId,imageIds(逗号隔开)。

然后我们的需求是
- 基于allGettyMeta文件进行过滤，remove kwIds为空的imageId
- 统计所有的imageId,kwId对的数量
- 统计每个关键词的图片量
- 获取所有的关键词，它的图片量大于300
- 从gettytopNumImagesOfKwId文件中获取所有的imageId， 这些imageId位于上一步获取的关键词的top5000中
- 基于上一步获取的imageId， 然后再结合allGettyMeta文件， 最终得到文件：finalImageKwIds， 格式：
gettyImageId,kwIds,url
- 最后一步是基于上一步产生的文件生成模型训练数据，格式是tfrecords的， 格式是：imageId,kwIds,imageBytes


