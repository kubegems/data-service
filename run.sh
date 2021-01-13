#!/bin/bash


run_service() {
    case $1 in
    "gateway")
        java -jar /dataservice/gateway/bigdata-dataservice-gateway.jar --spring.profiles.active=${2}
        ;;

    "quoto-config")
        java -jar /dataservice/quote-config/bigdata-dataservice-quoto-config.jar --spring.profiles.active=${2}
        ;;

    "quoto-roc")
        java -jar /dataservice/quote-roc/bigdata-dataservice-quoto-roc.jar --spring.profiles.active=${2}
        ;;

    *)
        echo "command not found; exit now!"
        exit 1
        ;;
    esac
}

run_service $@
