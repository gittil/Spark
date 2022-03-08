# Spark

- Utiliza o Metasote do Hive
- Não é preciso ter o Hive instalado para usar o Spark

### Tabela

- Persistente
- Objeto Tabular que reside em um banco de dados
- Pode ser gerenciado e consultado utilizando SQL
- Totalmente interoperável com DataFrame
- Ex: você pode transformar um DataFrame que importamos (Parquet, json, orc, csv) em tabela

### Tabelas Gerenciadas e Não Gerenciadas


**Gerenciadas: Spark gerecia dados e metadados**
- Armazenadas no warehouse do Spark
- Se excluirmos, tudo é apagado(dados e metadados)


**Não Gerenciadas (External): Spark apenas gerencia metadados**
- Informamos onde a tabela está (arquivo, por exemplo ORC)
- Se excluirmos, o Spark só exclui os metadados, dados permanecem onde estavam

# Views

- Mesmo conceito de banco de dados relacionais
- São um "alias" para uma tabela (por exemplo, vendas_rs pode mostrar vendas do estado já com filtro aplicado)
- Não contém dados

**GLOBAIS:** Visíveis em todas as sessões

**SESSÃO:** Visíveis apenas na própria sessão


