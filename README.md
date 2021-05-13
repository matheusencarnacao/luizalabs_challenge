# Luiza Labs challenge
Resolução do challenge para Data Engineer - Luiza Labs

## O Desafio

### Task 1
Leia o arquivo de texto wordcount.txt, e conte as palavras que contém até 10 letras. Conte
também quantas palavras com mais de 10 letras existem no texto.
Dataset: https://storage.googleapis.com/luizalabs-hiring-test/wordcount.txt

### Task 2
Leia o arquivo pedidos.csv e agrupe todos os cliente que fizeram mais de 2 compras nos dias
de black friday dos últimos três anos. Filtre todos os clientes que são menores de 30 anos e
coloque numa lista TODOS os códigos de pedido e a data em que foram efetuados. Adicione
também a idade do cliente.
Dataset: https://storage.googleapis.com/luizalabs-hiring-test/clientes_pedidos.csv

## Resolução

---

### Introdução

Iniciei o desenvolvimento e teste exploratório dos dados no Google Colab, e exportei para um jupyter notebook. 
O código exploratório se encontra dentro da pasta *notebook*.

### Aplicação

Testei e executei o teste no Spark localmente. O código está dividido em camadas:

- Business -> Regras de negócio atrelado ao challenge
- Service -> Pré tratamento dos dados e conversões de datas e campos
- Repository -> Leitura dos dados ou bases

## Pré requisitos

---

### 1.Spark

Para executar localmente, é necessário ter o spark baixado.

```
wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
```

ou baixar direto do [site](https://spark.apache.org/downloads.html)

---
### 2.Variáveis de ambiente

É necessário configurar a váriavel SPARK_HOME para executar a aplicação localmente

```
export SPARK_HOME="path/to/spark/root"
```

<br>
<br>
Também é necessário configurar a variavel de ambiente para o spark encontrar os dados (wordcount.txt e clientes_pedidos.csv)

```
export INPUT_PATH="local/to/res/files"
```

---

### 3.Python

É necessário ter as bibliotecas instaladas ou localmente ou em um ambiente virtual (venv)

Execute o código abaixo na máquina para instalar as dependencias local
```
pip install -r requirements.txt
```
Ou cria um virtualenv com o seguinte comando
```
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
deactivate
```
---

## Execução

Para executar a aplicação basta chamar o main.py passando como argumento o caminho da pasta de saida dos csv gerado.

```
python main.py ./
```

Após a execução será gerado os arquivos dentro de duas pastas com apenas uma partição. 

ex: clients_orders_bf.csv/part-00000-4b2d7c69-33ec-4ad9-9ef3-4d75b6de0643-c000.csv 

---

## Próximos passos

Minha idéia era poder executar a aplicação em um ambiente próximo ao produtivo. Porém tive uns problemas e por conta do tempo decidi não seguir com essa abordagem.

A idéia era subir um cluster com docker e executar o arquivo shell setup_and_submit.sh 
e enviar a aplicação ao cluster spark.

Execute o shell build.sh dentro de dockers para criar as imagens dos dockers
```
sh build.sh
```

Execute docker-compose up para subir todos os serviços
```
docker-compose up
```

Para parar e remover todos os serviços, execute:
```
docker-compose down
```

Para rodar a aplicação em ambiente produtivo, 
será necessário realizar algun ajuste para ele poder pegar o arquivo local ou do HDFS.
E também subir as dependencias para o spark-submit ou instalar o requirements.txt em todos os containers.
