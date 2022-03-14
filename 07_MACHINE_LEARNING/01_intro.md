# Machile Learning no Spark

## Bibliotecas:
- spark.mllib
- spark.ml

* ML baseado em RDD está descontinuado
* Implementações todas em Dataframes

## Tradicionalmente:
- Variáveis independentes são colunas distintas

- Variável Dependente: outra coluna

## No Spark
- Normalmente todas as variáveis independentes devem compor uma mesma coluna
- Cria-se um vetor único, que é adicionado em nova coluna no DataFrame

### One HotEncoding
- Machine Learning suporta apenas números
- Atributos categóricos devem ser transformados
- Se o atributo tiver muitos valores, muitas colunas serão criadas
- Spark permite o uso de matriz esparsa
- Muitos valores zero que não são registrados

### Formulas no R
- R permite definir modelo através de fórmula
- [variável dependente]~[variável independente]
- Ponto define todos os atributos - variável dependente
- Spark implemente Rformula
- Aplica One HotEncoding e combina variáveis independentes em uma única coluna


## Pipelines
- Transfomer: Transforma um DF em outro DF
- Estimator: Fit em DF para produzir um Transformer
- Pipeline: conecta Transformers e Estimators para produzir modelo
- Parâmetros: Transformers e Estimators compartilham uma API para definir parâmetros

