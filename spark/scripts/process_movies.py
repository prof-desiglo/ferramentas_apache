#!/usr/bin/env python3
"""
Script para processar movies.csv no HDFS
Suporta input e output no HDFS
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    # Parse argumentos
    input_path = None
    output_path = None
    master_url = "spark://spark-master:7077"
    hadoop=None
    
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == '--input' and i + 1 < len(sys.argv):
            input_path = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--output' and i + 1 < len(sys.argv):
            output_path = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--master' and i + 1 < len(sys.argv):
            master_url = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--hadoop' and i + 1 < len(sys.argv):
            hadoop = sys.argv[i + 1]
            i += 2
        else:
            i += 1
    
    if not input_path:
        print("""
ERRO: Argumento --input é obrigatório!

 USO:
  spark-submit process_movies.py --input <caminho> [--output <caminho>]

 EXEMPLOS:
  # Básico
  spark-submit process_movies.py --input /user/hadoop/movies.csv
  
  # Com output
  spark-submit process_movies.py --input /user/hadoop/movies.csv --output /data/results
  
  # Com HDFS explícito
  spark-submit process_movies.py --input hdfs://namenode:9000/user/hadoop/movies.csv --output hdfs://namenode:9000/data/results
        """)
        sys.exit(1)
    
    print(f"""
 INICIANDO PROCESSAMENTO
├─  Input:  {input_path}
├─  Output: {output_path if output_path else 'Não salvar'}
└─  Master: {master_url}
    """)
    
    # Configuração HDFS
    hdfs_base_url = f"hdfs://{hadoop}"
    
    # Criar SparkSession
    spark = SparkSession.builder \
        .appName("MoviesAnalysis") \
        .master(master_url) \
        .config("spark.hadoop.fs.defaultFS", hdfs_base_url) \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    print(f Spark configurado para HDFS: {hdfs_base_url}")
    
    try:
        # 1. CARREGAR DADOS
        print(f"\n Carregando: {input_path}")
        
        # Adicionar hdfs:// se não tiver
        if not input_path.startswith("hdfs://"):
            input_path = f"{hdfs_base_url}{input_path}"
        
        # Tentar diferentes formatos
        try:
            # Método 1: Com header
            df = spark.read \
                .option("header", "true") \
                .option("delimiter", ",") \
                .option("inferSchema", "true") \
                .csv(input_path)
            print(" Carregado com header")
        except:
            # Método 2: Sem header
            df = spark.read \
                .option("header", "false") \
                .option("delimiter", ",") \
                .option("inferSchema", "true") \
                .csv(input_path) \
                .selectExpr("_c0 as movieId", "_c1 as title", "_c2 as genres")
            print(" Carregado sem header")
        
        # 2. ANÁLISE
        total_filmes = df.count()
        print(f"\n TOTAL DE FILMES: {total_filmes:,}")
        
        print("\n AMOSTRA (10 primeiros):")
        df.select("movieId", "title", "genres").show(10, truncate=False)
        
        # 3. PROCESSAR GÊNEROS
        print("\n CONTAGEM DE FILMES POR GÊNERO:")
        print("=" * 60)
        
        from pyspark.sql.functions import explode, split
        
        # Filtrar filmes sem gênero
        df_com_generos = df.filter(col("genres") != "(no genres listed)")
        
        # Explodir gêneros e contar
        generos_df = df_com_generos \
            .withColumn("genero", explode(split(col("genres"), "\\|"))) \
            .select("movieId", "title", "genero")
        
        contagem_generos = generos_df.groupBy("genero") \
            .agg(
                count("*").alias("quantidade_filmes"),
                round((count("*") / total_filmes * 100), 2).alias("percentual")
            ) \
            .orderBy(desc("quantidade_filmes"))
        
        # Mostrar resultados
        contagem_generos.show(truncate=False)
        
        # 4. ESTATÍSTICAS
        print("\n ESTATÍSTICAS:")
        print(f"• Gêneros únicos: {contagem_generos.count()}")
        print(f"• Total de filmes: {total_filmes:,}")
        
        top_genero = contagem_generos.first()
        if top_genero:
            print(f"• Gênero mais popular: {top_genero['genero']} "
                  f"({top_genero['quantidade_filmes']} filmes, {top_genero['percentual']}%)")
        
        # 5. SALVAR RESULTADOS (se solicitado)
        if output_path:
            print(f"\n SALVANDO RESULTADOS...")
            
            # Adicionar hdfs:// se não tiver
            if not output_path.startswith("hdfs://"):
                output_path = f"{hdfs_base_url}{output_path}"
            
            # Salvar contagem de gêneros
            contagem_generos.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{output_path}/contagem_generos")
            
            # Salvar filmes com ano extraído
            from pyspark.sql.functions import regexp_extract
            
            df_com_ano = df.withColumn(
                "ano", 
                regexp_extract(col("title"), r"\((\d{4})\)", 1).cast("int")
            )
            
            df_com_ano.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{output_path}/filmes_com_ano")
            
            print(f" Resultados salvos em HDFS: {output_path}")
            
            # Verificar se salvou
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            output_hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(output_path)
            
            if fs.exists(output_hdfs_path):
                print(f"\n Conteúdo salvo:")
                for f in fs.listStatus(output_hdfs_path):
                    print(f" {f.getPath().getName()}")
        
        print("\n" + "=" * 60)
        print(" PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()