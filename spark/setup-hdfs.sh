hdfs dfs -mkdir -p /user/hadoop
hdfs dfs -put /tmp/movies.csv /user/hadoop/
hdfs dfs -chmod 644 /user/hadoop/movies.csv
echo 'Arquivo no HDFS:'
hdfs dfs -ls /user/hadoop/
echo -e '\nPrimeiras 5 linhas:'
hdfs dfs -cat /user/hadoop/movies.csv | head -5