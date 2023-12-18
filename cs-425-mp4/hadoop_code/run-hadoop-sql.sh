# hadoop fs -put traffic.csv /traffic.csv
hadoop fs -rm -r /sql-output
rm -rf /home/hadoopuser/sql-output
col="Interconne"
# X="Radio"
X="^Radio|Fiber|Fiber/Radio$"
# col="Stories"
# X="3"

start=`date +%s%N | cut -b1-13`

hadoop jar /home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-mapper "/home/hadoopuser/sql_map.py $col $X" \
-input /traffic.csv \
-output /sql-output
end=`date +%s%N | cut -b1-13`

echo Execution time was `expr $end - $start` milliseconds.

# echo Execution time was `expr $(( ($end - $start) / 1000000))` milliseconds.

hadoop fs -get /sql-output /home/hadoopuser
# hadoop fs -get /mr1 /home/hadoopuser