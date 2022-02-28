package com.cloudminds.bigdata.dataservice.quoto.roc.service;

import apijson.JSON;
import apijson.Log;
import apijson.framework.APIJSONController;
import apijson.orm.Parser;
import com.alibaba.fastjson.JSONObject;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

@Service
public class SaveAccessHistory extends APIJSONController {

    @Override
    public Parser<Long> newParser(HttpSession session, apijson.RequestMethod method) {
        return super.newParser(session, method).setNeedVerify(false); // TODO 这里关闭校验，方便新手快速测试，实际线上项目建议开启
    }

    @Async("taskExecutor")
    public void saveAccessHistory(String token, String service_path, String table_alias, String response, HttpSession session) {
        //解析response
        JSONObject responseJson = JSON.parseObject(response);
        if (responseJson == null) {
            return;
        }
        String executeSql = responseJson.getString("execute_sql").replaceAll("\"", "");
        //Log.i("SaveAccessHistory", "token:" + token + " service_path:" + service_path + " table_alias:" + table_alias + " execute_sql:" + executeSql + " success:" + responseJson.getString("ok") + " msg:" + responseJson.getString("msg"));
        String requestPost = "{\"@database\":\"DATASERVICE\",\"@schema\":\"bigdata_dataservice\",\"dataservice_access_history\": {\"token\":\"" + token + "\",\"service_path\":\"" + service_path +
                "\",\"table_alias\":\"" + table_alias + "\",\"execute_sql\":\"" + executeSql + "\",\"success\":" + responseJson.getString("ok") + ",\"msg\":\"" + responseJson.getString("msg") + "\"}}";
        post(requestPost, session);
    }
}
