# Spark

## Vantagens

- Memória
- Operação em Cluster
- Particionamento (divisão de dados)
- Paralelismo
- Redundância

## Particionamento

- Por padrão dados são particionados de acordo com o número de núcleos
- Cada particição fica em um nó e tem uma task
- Podemos particionar explicitamente em disco (partitionBy)
- Ou em memória: repartition() ou coalesce()

## Shufle
Redistribuição de dados entre partições

## Bucketing

- Semelhante a particionamento, porém com número fixo de partições
- Ideal para coluna com alta cardinalidade
- Pode ser usado em conjunto com particionamento



