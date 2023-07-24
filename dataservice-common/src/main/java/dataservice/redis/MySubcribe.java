package dataservice.redis;

import dataservice.controller.DataServiceControl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MySubcribe implements MessageListener {

    private DataServiceControl dataServiceControl;

    public MySubcribe(DataServiceControl dataServiceControl) {
        this.dataServiceControl=dataServiceControl;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        if (message == null) {
            log.info("接收消息内容为空,不处理");
            return;
        }
        String messageInfo = message.toString().replaceAll("\"","");
        if (messageInfo.equals("reflushDbInfo")) {
            log.info("接收消息reflushDbInfo");
            dataServiceControl.initAPIJSONApplication();
        } else if (messageInfo.equals("reflushConfig")) {
            log.info("reflushConfig");
            dataServiceControl.refreshConfig();
        } else {
            log.info("接收不用处理的消息：" + messageInfo);
            return;
        }
    }
}
