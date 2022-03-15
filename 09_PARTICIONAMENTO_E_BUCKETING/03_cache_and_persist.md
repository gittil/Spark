# Cache e Persist

- Reutilização de Dataframe
- Cache - padrão em memória
- Persist - definido pelo usuário

## StorageLevel
- MEMORY_ONLY: Padrão para RDD, porém se não caber na memória será reprocessado a cada consulta
- MEMORY_AND_DISK: Padrão para Dataframe. Armazena as partições que não cabem em memória, em disco.

