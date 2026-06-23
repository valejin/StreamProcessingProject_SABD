# Avvia del Docker Compose
cd /mnt/c/Users/Valen/Desktop/SreamProcessingProject_SABD/SreamProcessingProject_SABD/docker
docker compose up -d

# Apri il browser su http://localhost:8081 — dovresti vedere la Flink Web UI 
# con 1 TaskManager e 4 slot disponibili.

# Lancio il producer
cd /mnt/c/Users/Valen/Desktop/SreamProcessingProject_SABD/SreamProcessingProject_SABD
python3 producer/kafka_producer.py
## Lancio il producer fast (dura 5 min replay)
TIME_SCALE_FACTOR=360000 python3 kafka_producer.py 


## crea il topic flight (da fare dopo ogni docker compose up)
docker exec kafka kafka-topics \
--bootstrap-server kafka:29092 \
--create --topic flights \
--partitions 1 \
--replication-factor 1


# Query 1 oppure su web UI con Flink
docker exec \
-e KAFKA_BROKER=kafka:29092 \
flink-jobmanager \
flink run \
-c it.uniroma2.sabd.query1.Query1Job \
/opt/flink/jobs/SreamProcessingProject_SABD-1.0-SNAPSHOT.jar
# alternativa con UI
Quando finisce, vai su http://localhost:8081 → Submit New Job → + Add New e carica il file:
C:\Users\Valen\Desktop\SreamProcessingProject_SABD\SreamProcessingProject_SABD\target\SreamProcessingProject_SABD-1.0-SNAPSHOT.jar
Poi imposta Entry Class: it.uniroma2.sabd.query1.Query1Job e clicca Submit.

## per monitorare l'output man mano che le finestre si chiudono
docker exec flink-jobmanager tail -f /results/q1_output.csv

## ricompilare il progetto (da fare ogni volta che cambio il codice)
cd /mnt/c/Users/Valen/Desktop/SreamProcessingProject_SABD/SreamProcessingProject_SABD
mvn clean package -DskipTests

# Monitora quante righe ci sono (rilancia ogni 30s finché il numero non cambia più)
docker exec flink-taskmanager wc -l /results/q1_output.csv