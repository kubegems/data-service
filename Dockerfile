FROM openjdk:jdk-oraclelinux8
COPY run.sh /run.sh
COPY gateway/target/bigdata-dataservice-gateway.jar /dataservice/gateway/
COPY quoto-config/target/bigdata-dataservice-quoto-config.jar /dataservice/quoto-config/
COPY quoto-roc/target/bigdata-dataservice-quoto-roc.jar /dataservice/quoto-roc/
COPY quoto-chatbot/target/bigdata-dataservice-quoto-chatbot.jar /dataservice/quoto-chatbot/