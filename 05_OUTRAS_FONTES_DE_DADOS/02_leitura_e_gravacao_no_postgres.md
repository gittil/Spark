## Leitura e gravação de dados no PostgreSQL

- Abrir um novo terminal

cd Downloads/

ls

chamando o pyspark passando como parâmetro o driver para conectar no postgre

pyspark --jars post...

from pyspark import SparkSession


## lendo no PostgreSQL

resumo = spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/vendas").option("dbtable","Vendas").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").load()

resumo.show()

clientes = spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/vendas").option("dbtable","Clientes").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").load()

clientes.show()

## Gravando no Postgre

vendadata = resumo.select("data","total")

vendadata.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/vendas").option("dbtable","Vendadata").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").save()

### Para confirmar que a tabela nova foi criada

- abrir o terminal do postgres
- \dt
- select * from vendadata;
