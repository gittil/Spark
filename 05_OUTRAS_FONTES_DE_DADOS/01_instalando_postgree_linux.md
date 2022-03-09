# Instalando o Postgree SQL no Linux

Atualizando os pacotes

sudo apt-get update

Instalando o PostgreeSQL 

sudo apt-get install postgresql-12

Logando no SGBD

sudo -u postgres psql

definindo senha para o usuário

\password

Criando a DB

create database vendas;

Entrando na DB

\c vendas;

Rodando os scripts

\i /home/douglas/download/demo/1.CreateTable.sql

**rodar os scripts de 1 a 6 para popular o banco**

para ver se está tudo ok:

\dt

select * from vendedores;

**Download do drive JDBC**

https://jdbc.postgresql.org/download.html



