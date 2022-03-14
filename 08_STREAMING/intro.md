# Spark Structured Streaming

### O que é Processamento Streaming?

- Processamento de dados contínuo
- Em tempo real, ou próximo a tempo real


### Por que?
- Analisar dados é um processo de transformação: Dados > Processamento > Informação
- A informação tem um valor
- O valor de qualquer informação está relacionada diretamente ao tempo!

### Aplicações
- Operações de bolsas de valores
- Monitoramente de Rede
- Aplicativos de Transporte
- Cercamento Eletrônico

### Structured Streaming

- Segunda geração de processamento de streaming do Spark (Dstream foi a primeira)
- Garantia de processamento único de cada registro (end-to-end exctly-once guarantess)

**Modos de saída**
- append: só novas linhas. Suporta apenas consultas stateless
- update: apenas linhas que foram atualizadas
- complete: toda a tabela é atualizada

**Trigger**
- Default: dispara quando o micro batch termina
- Tempo
- Once: apenas uma única vez
- Continuous: processamento contínuo
- stop( ): para o processo

### Checkpointdir
- Diretório onde o estado de andamento é salvo
- Se você parar o processo e reiniciar com o mesmo diretório, ele segue de onde parou

### Métodos semelhantes aos de batch
- readstream em vez de read
- writestream em vez de write

### Sources e Sinks que não tem suporte

- Métodos de batch podem ser usados (read, write):
    - foreachbatch: opera no micro batch
    - foreach: opera a cada linha
- Algumas garantias são perdidas, por exemplo, exactly-once


