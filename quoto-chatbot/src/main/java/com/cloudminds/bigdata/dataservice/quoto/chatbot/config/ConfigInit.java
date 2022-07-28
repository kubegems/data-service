package com.cloudminds.bigdata.dataservice.quoto.chatbot.config;

import apijson.framework.APIJSONApplication;
import apijson.framework.APIJSONCreator;
import apijson.orm.SQLConfig;
import com.cloudminds.bigdata.dataservice.quoto.chatbot.controller.ChatbotQuotoControl;
import com.cloudminds.bigdata.dataservice.quoto.chatbot.redis.RedisUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class ConfigInit implements ApplicationRunner {
    public static Map<String, String> dbInfo=new HashMap<String, String>();
    @Autowired
    private ChatbotQuotoControl chatbotQuotoControl;
    @Value("${dbUrl}")
    private String dbUrl;
    @Value("${dbAccount}")
    private String dbAccount;
    @Value("${dbPassword}")
    private String dbPassword;
    @Value("${configDbUrl}")
    private String configDbUrl;
    @Value("${configDbAccount}")
    private String configDbAccount;
    @Value("${configDbPassword}")
    private String configDbPassword;
    @Override
    public void run(ApplicationArguments args) throws Exception {
        dbInfo.put("dbUrl", dbUrl);
        dbInfo.put("dbAccount", dbAccount);
        dbInfo.put("dbPassword", dbPassword);
        dbInfo.put("configDbUrl", configDbUrl);
        dbInfo.put("configDbAccount", configDbAccount);
        dbInfo.put("configDbPassword", configDbPassword);
        APIJSONApplication.DEFAULT_APIJSON_CREATOR = new APIJSONCreator() {
            @Override
            public SQLConfig createSQLConfig() {
                return new DBSQLConfig(dbUrl, dbAccount,dbPassword, configDbUrl,configDbAccount, configDbPassword);
            }
        };
        APIJSONApplication.init(false);  // 4.4.0 以上需要这句来保证以上 static 代码块中给 DEFAULT_APIJSON_CREATOR 赋值会生效
        chatbotQuotoControl.refush();
        System.out.println("\n\n<<<<<<<<<<<<<<<<<<<<<<<<< APIJSON 启动完成，试试调用自动化 API 吧 ^_^ >>>>>>>>>>>>>>>>>>>>>>>>\n");
    }
}
