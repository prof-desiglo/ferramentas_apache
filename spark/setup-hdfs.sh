hdfs dfs -mkdir -p /data/results
echo 'Criada pasta de resultados!'
hdfs dfs -mkdir -p /user/spark
echo 'Criada pasta do spark!'
hdfs dfs -put /tmp/movies.csv /user/spark/
echo 'Arquivo no HDFS:'
hdfs dfs -ls /user/spark/
echo -e '\nPrimeiras 5 linhas:'
hdfs dfs -cat /user/spark/movies.csv | head -5
hdfs dfs -chown -R spark:spark /user/spark/
hdfs dfs -chown -R spark:spark /data/results
hdfs dfs -chmod -R 777 /data/results
hdfs dfs -chmod 644 /user/spark/movies.csv
echo 'Permissões corrigidas!'