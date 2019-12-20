# vcg_study
some demo to study 

spark pyspark相关文档：
https://spark.apache.org/docs/latest/rdd-programming-guide.html



每个文件代表一个基于hadoop yarn的spark 应用程序。
运行的方式是直接以用户stuff登陆服务器：172.16.241.100，
然后在/data5/stuff/下面创建你自己的目录，
然后git clone https://github.com/jasstionzyf/vcg_study.git  && cd ./vcg_study
nohup spark-submit --master yarn --name yufei.zhao-gettyml  
--py-files hdfs://172.16.241.100:9000/data/stuff/vcgImageAI.zip,hdfs://172.16.241.100:9000/data/stuff/vcgPylibary.tar.gz 
--deploy-mode cluster   --num-executors 10 --executor-memory 4G  gettyml.py> ./out &




运行之前务必： git pull 更新项目代码


