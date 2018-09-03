#!/bin/sh

echo "The application will start in ${JHIPSTER_SLEEP}s..." && sleep ${JHIPSTER_SLEEP}
exec java ${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom -jar "${HOME}/app.war" \
    --application.port=${PORT} \
    --application.eureka.url=${EUREKA_URL} \
    --application.database.url=${DATABASE_URL} \
    --application.database.username=${DATABASE_USERNAME} \
    --application.database.password=${DATABASE_PASSWORD} \
    --application.kafka.brokers=${KAFKA_BROKERS} \
    --application.kafka.zk-nodes=${KAFKA_ZK_NODES} \
    --application.mongodb.url=${MONGODB_URL} \
    --application.controller.url=${CONTROLLER_URL} \
    --application.controller.username=${CONTROLLER_USERNAME} \
    --application.controller.password=${CONTROLLER_PASSWORD} \
    --application.elasticsearch.cluster-name=${ES_CLUSTER_NAME} \
    --application.elasticsearch.cluster-nodes=${ES_CLUSTER_NODES} \
    "$@"
