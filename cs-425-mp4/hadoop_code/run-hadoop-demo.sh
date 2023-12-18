hadoop fs -rm -r /demo-output
hadoop fs -rm -r /mr1
rm -rf /home/hadoopuser/demo-output
rm -rf /home/hadoopuser/mr1
X="Radio/Fiber"

hadoop jar /home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-mapper "/home/hadoopuser/hadoop_maple.py $X" \
-reducer /home/hadoopuser/hadoop_juice.py \
-input /traffic.csv \
-output /mr1

hadoop jar /home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-mapper "/home/hadoopuser/hadoop_maple2.py $X" \
-input /mr1 \
-output /demo-output

hadoop fs -get /demo-output /home/hadoopuser
# hadoop fs -get /mr1 /home/hadoopuser