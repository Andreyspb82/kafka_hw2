# Kafka Самостоятельная работа №2
---
### Проект состоит из 3 составляющие:
* файла docker-compose (для создания брокера)
* модуля "hw2-topology" который реализует потоковую логику обработки входящих сообщений
* модуля "hw2-producer" для отправки тестовых сообщений

### Запуск приложения и проверка его работы:

#### 1) С помощью файла docker-compose создается кластер из 1 брокера и интерфейса для взаимодействия с кафкой
#### В консоли необходимо выполнить следующие команды:
* **"docker compose up -d"**  (для создания брокера);
* **"docker ps"** чтобы узнать id контейнера брокер;
* **"docker exec -it <container_id_kafka> /bin/sh"**, где **<container_id_kafka>** id брокеров кафка, чтобы попасть в консоль брокера;
* В консоли взаимодествия с брокером кафка необходимо выполнить следующие команды:
  * Выполнить команду **"cd /opt/bitnami/kafka/bin"** для переходв в папку с файлом **kafka-topics.sh**;
  * Выполнить команду для создания топика **"messages"** - **"kafka-topics.sh --create --topic messages --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"**;
  * Выполнить команду для создания топика **"blocked_users"** - **"kafka-topics.sh --create --topic blocked_users --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"**;
  * Выполнить команду для создания топика **"censure_text"** - **"kafka-topics.sh --create --topic censure_text --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"**;
  * Выполнить команду для создания топика **"filtered_message"** - **"kafka-topics.sh --create --topic filtered_message --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"**;
    
#### 2) В модуле "hw2-topology" в классе "BlockingCensureApplication" запусить метод *"main"*
Потребитель данных подключится к брокеру и будет в режиме ожидания получения сообщений.

#### 3) В модуле "hw2-producer", в классе "Hw2ProducerApplication" запусить метод *"main"*
С помощью **insomnia**, продюсеру необходимо отправить тестовые данные.

Тестовые данные и эндпоинты находятся в файле **"hw2_test_data.txt"**

### Описание работы потоковой обработки описано в классах модуля **""hw2-topology""**:
* в класее **"BlockingCensureApplication"**
* в классах процессоров в пакете **"processor"**

### Исоходные и обработанные сообщения можно посмотреть в следующих топиках:
* **"messages"** - сообщения поступающие от продюсера до потоково обработки;
* **"filtred-message"** - сообщения прошедшие потоковую обработку.
