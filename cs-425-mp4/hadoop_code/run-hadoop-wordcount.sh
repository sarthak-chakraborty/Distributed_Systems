hadoop jar /home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-mapper /home/hadoopuser/python-wordcount/mapper.py \
-reducer /home/hadoopuser/python-wordcount/reducer.py \
-input /traffic.csv \
-output /output.txt