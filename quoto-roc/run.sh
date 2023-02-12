#!/bin/bash


run_service() {
    case $1 in
    "gateway")
        java -jar -Duser.timezone=Asia/Shanghai /dataservice/gateway/bigdata-dataservice-gateway.jar --spring.profiles.active=${2}
        ;;

    "quoto-config")
        java -jar -Duser.timezone=Asia/Shanghai /dataservice/quoto-config/bigdata-dataservice-quoto-config.jar --spring.profiles.active=${2}
        ;;
        
    "quoto-chatbot")
        java -jar -Duser.timezone=Asia/Shanghai /dataservice/quoto-chatbot/bigdata-dataservice-quoto-chatbot.jar --spring.profiles.active=${2}
        ;;

    "quoto-roc")
        java -jar -Xmx3g -Xms3g -Duser.timezone=Asia/Shanghai /dataservice/quoto-roc/bigdata-dataservice-quoto-roc.jar --spring.profiles.active=${2}
        ;;
		
	"standard-manage")
        java -jar -Duser.timezone=Asia/Shanghai /dataservice/standard-manage/bigdata-dataservice-standard-manage.jar --spring.profiles.active=${2}
        ;;
		
	"quoto-manage")
        java -jar -Duser.timezone=Asia/Shanghai /dataservice/quoto-manage/bigdata-dataservice-quoto-manage.jar --spring.profiles.active=${2}
        ;;

    *)
        echo "command not found; exit now!"
        exit 1
        ;;
    esac
}

run_service $@
