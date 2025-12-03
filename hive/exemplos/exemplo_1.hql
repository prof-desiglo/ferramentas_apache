CREATE DATABASE IF NOT EXISTS vendas;

USE vendas;

CREATE TABLE IF NOT EXISTS clientes (
    id INT,
    nome STRING,
    idade INT,
    cidade STRING,
    data_cadastro DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

show tables;

INSERT INTO clientes VALUES
(1, 'João Silva', 30, 'São Paulo', '2024-01-15'),
(2, 'Maria Santos', 25, 'Rio de Janeiro', '2024-02-20');

SELECT * FROM clientes;
