package dataservice.config;

import dataservice.controller.DataServiceControl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ConfigInit implements ApplicationRunner {

    @Autowired
    private DataServiceControl dataServiceControl;
    @Override
    public void run(ApplicationArguments args) throws Exception {
        dataServiceControl.initAPIJSONApplication();
        dataServiceControl.refreshConfig();
        System.out.println("\n\n<<<<<<<<<<<<<<<<<<<<<<<<< APIJSON 启动完成，试试调用自动化 API 吧 ^_^ >>>>>>>>>>>>>>>>>>>>>>>>\n");
    }
}
