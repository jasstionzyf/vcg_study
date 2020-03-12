# vcg_study
此项目主要目的：
- 结合实际工作中的需求以及相应的基于spark on yarn的代码实现， 让大家意识到spark 在大数据分析与处理相关任务中的效率， 从而引导大家去持续的探索和学习spark技术栈。
- 提供必要的辅助类， 避免自己花费时间和精力去部署和维护spark on yarn集群，可以直接加上两行固定的代码就可以直接和spark on yarn集群进行交互，运行自己的基于spark 的数据处理与分析的代码。





服务地址：
- hdfs cluster: http://172.16.241.100:50070/explorer.html#/data
- yarn cluster: http://172.16.241.100:8088/cluster (http://172.16.241.100:8088/ui2/#/cluster-overview)

本项目的所有的spark相关代码都是基于pyspark（python 语言实现的spark客户libary）
spark pyspark相关文档：
https://spark.apache.org/docs/latest/rdd-programming-guide.html



hadoop yarn docker run:
cd /data/apps/hadoop-3.2.1
yarn jar ./share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.2.1.jar \
       -jar ./share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.2.1.jar \
       -shell_env YARN_CONTAINER_RUNTIME_TYPE=docker \
       -shell_env YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=local/imageai_bigdata \
       -shell_command 'echo $PWD' \
       -container_resources memory-mb=3072,vcores=1,yarn.io/gpu=1  \
       -num_containers 1
shell_command 可以指定任何你想运行的系统支持的命令，比如：nvidia-smi  确定hadoop yarn给你分配的容器里面有一个gpu资源





程序部署运行步骤：
- 完成自己的spark 程序代码：比如：test.py
- 以用户stuff登陆服务器：172.16.241.100，在/data5/stuff/下面创建你自己的目录
- cd 到自己上一步创建的目录， 然后运行命令：git clone https://github.com/jasstionzyf/vcg_study.git  && cd ./vcg_study, 如果已经clone ， 运行git pull更新代码
- 直接运行代码：nohup spark-submit --master yarn --name {your_name}-test.py  
--py-files hdfs://172.16.241.100:9000/data/stuff/vcgImageAI.zip,hdfs://172.16.241.100:9000/data/stuff/vcgPylibary.tar.gz 
--deploy-mode cluster   --num-executors 1 --executor-cores 1 --executor-memory 2G  test.py> ./out &
- 程序运行完成之后，日志会保存到hdfs上面，找到你的程序的appId， 然后运行
  hadoop fs -get /data/yarn/logs/stuff/logs-tfile/{appId}  /tmp/
  到/tmp/{appId}/下面就可以查看所有程序的完整日志
  
  
  
  

  
  
  











