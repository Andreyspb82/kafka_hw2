# Kafka Самостоятельная работа №2
---
### Проект состоит из 3 составляющие:
* файла docker-compose (для создания брокера)
* модуля "hw2-topology" который реализует потоковую логику обработки входящих сообщений
* модуля "hw2-producer-test-messages" для создания и отправки тестовых сообщений

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

#### 3) В модуле "hw2-producer-test-messages", для создания текстовых сообщений, необходимо запустить методы *"main"* в классах, в следущем порядке:
* **"CreateMessages1"**;
* **"CreateMessages2BlockedUsers"**;
* **"CreateMessages3CensureText1"**;
* **"CreateMessages4"**;
* **"CreateMessages5CensureText2"**;
* **"CreateMessages6"**.

Назначение текстовых сообщений описано в классах. 

### Описание работы потоковой обработки описано в классах модуля **""hw2-topology""**:
* в класее **"BlockingCensureApplication"**
* в классах процессоров в пакете **"processor"**
