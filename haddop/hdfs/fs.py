from hdfs import InsecureClient

client = InsecureClient('http://namenode:9870', user='hadoop')

# Upload
client.upload('/user/hadoop/teste.txt', 'local.txt')

# Download
client.download('/user/hadoop/teste.txt', 'baixado.txt')

# Listar diretório
print(client.list('/user/hadoop/'))

# Ler arquivo como texto
with client.read('/user/hadoop/teste.txt', encoding='utf-8') as f:
    print(f.read())
