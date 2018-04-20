#!/usr/bin/env bash

SPARK_HOME="/Users/wangzhiwei/opt/spark-2.1.0-bin-hadoop2.7"
master="spark://zhiweideMBP.local:7077"
#master="local[2]"
#input="/Users/wangzhiwei/IdeaProjects/FDmining/dataset/bots_200_15.csv"
#input="/Users/wangzhiwei/IdeaProjects/FDmining/dataset/bots_200_10.csv"
#input="/Users/wangzhiwei/IdeaProjects/FDmining/dataset/bots_200_5.csv"
input="/Users/wangzhiwei/IdeaProjects/FDmining/dataset/bots_1m_10.csv"
output="/Users/wangzhiwei/IdeaProjects/FDmining/results"
exjar="/Users/wangzhiwei/IdeaProjects/FDmining/out/artifacts/fdmining_jar/fdmining.jar"

${SPARK_HOME}/bin/spark-submit \
--master ${master} \
--num-executors 2 \
--executor-memory 2g \
--executor-cores 2 \
${exjar} \
${input} \
${output} \
2> debug.log > output.log
