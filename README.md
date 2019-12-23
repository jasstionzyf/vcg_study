# vcg_study
此项目主要目的：
- 结合实际工作中的需求以及相应的基于spark on yarn的代码实现， 让大家意识到spark 在大数据分析与处理相关任务中的效率， 从而引导大家去持续的探索和学习spark技术栈。
- 提供必要的辅助类， 避免自己花费时间和精力去部署和维护spark on yarn集群，可以直接加上两行固定的代码就可以直接和spark on yarn集群进行交互，运行自己的基于spark 的数据处理与分析的代码。





服务地址：
- hdfs cluster: http://172.16.241.100:50070/explorer.html#/data
- yarn cluster: http://172.16.241.100:8088/cluster

本项目的所有的spark相关代码都是基于pyspark（python 语言实现的spark客户libary）
spark pyspark相关文档：
https://spark.apache.org/docs/latest/rdd-programming-guide.html


程序部署运行步骤：
- 完成自己的spark 程序代码：比如：test.py
- 以用户stuff登陆服务器：172.16.241.100，在/data5/stuff/下面创建你自己的目录
- cd 到自己上一步创建的目录， 然后运行命令：git clone https://github.com/jasstionzyf/vcg_study.git  && cd ./vcg_study, 如果已经clone ， 运行git pull更新代码
- 直接运行代码：nohup spark-submit --master yarn --name {your_name}-test.py  
--py-files hdfs://172.16.241.100:9000/data/stuff/vcgImageAI.zip,hdfs://172.16.241.100:9000/data/stuff/vcgPylibary.tar.gz 
--deploy-mode cluster   --num-executors 1 --executor-cores 1 --executor-memory 2G  test.py> ./out &
- 进入http://172.16.241.100:8088/cluster/apps/RUNNING 查看自己提交程序的运行相关日志










