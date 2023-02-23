package com.cloudminds.bigdata.dataservice.quoto.chatbot.service;

import apijson.JSON;
import apijson.framework.APIJSONController;
import apijson.orm.Parser;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.chatbot.entitys.TokenInfo;
import com.cloudminds.bigdata.dataservice.quoto.chatbot.redis.RedisUtil;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ExtendFunctionService extends APIJSONController {
    @Autowired
    private RedisUtil redisUtil;
    String serviceName = "roc";
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

    @HystrixCommand(fallbackMethod = "getTokenMapTimeout",commandProperties =
            {@HystrixProperty(name = "execution.isolation.thread.timeoutInMilliseconds",value = "1000")})
    public TokenInfo getTokenMap() {
        Object token_map_object = redisUtil.get(serviceName + "_token");
        TokenInfo tokenInfo = new TokenInfo();
        tokenInfo.setToken(token_map_object);
        tokenInfo.setRedis(true);
        return tokenInfo;
    }

    public TokenInfo getTokenMapTimeout() {
        TokenInfo tokenInfo = new TokenInfo();
        tokenInfo.setRedis(false);
        try {
            Map<String, String> userTokenCkMap = new HashMap<>();
            //加载超级账户
            String request = "{\"@database\":\"DATASERVICE\",\"@schema\":\"bigdata_dataservice\",\"[]\":{\"Token\": {\"@sql\":\"select token from bigdata_dataservice.user_token where `tables`='0' limit 100000\"},\"count\":100000}}";
            JSONObject object = JSON.parseObject(get(request, null));
            List<JSONObject> tokens = JSONArray.parseArray(object.get("[]").toString(), JSONObject.class);
            for (int n = 0; n < tokens.size(); n++) {
                JSONObject token = JSONObject.parseObject(tokens.get(n).getString("Token"), JSONObject.class);
                userTokenCkMap.put(token.getString("token"), "ALL");
            }
            //加载其它权限账户
            request = "{\"@database\":\"DATASERVICE\",\"@schema\":\"bigdata_dataservice\",\"[]\":{\"Token\": {\"@sql\":\"select t.token,GROUP_CONCAT(CONCAT_WS('.',Database_info.service_path,Table_info.table_alias)) as des from (SELECT a.token,SUBSTRING_INDEX( SUBSTRING_INDEX( a.`tables`, ',', b.help_topic_id + 1 ), ',',- 1 ) AS table_id FROM bigdata_dataservice.user_token a JOIN mysql.help_topic AS b ON b.help_topic_id < ( length( a.`tables` ) - length( REPLACE ( a.`tables`, ',', '' ) ) + 1 ) where a.is_delete=0 and a.state=1) t LEFT JOIN bigdata_dataservice.Table_info on t.table_id=Table_info.id LEFT JOIN bigdata_dataservice.Database_info ON Table_info.database_id=Database_info.id LEFT JOIN bigdata_dataservice.Db_info ON Database_info.db_id=Db_info.id where Db_info.db_name='Clickhouse' group BY token limit 100000\"},\"count\":100000}}";
            object = JSON.parseObject(get(request, null));
            tokens = JSONArray.parseArray(object.get("[]").toString(), JSONObject.class);
            for (int n = 0; n < tokens.size(); n++) {
                JSONObject token = JSONObject.parseObject(tokens.get(n).getString("Token"), JSONObject.class);
                userTokenCkMap.put(token.getString("token"), token.getString("des"));
            }
            tokenInfo.setToken(userTokenCkMap);
        }catch (Exception e){
            e.printStackTrace();
        }
        return tokenInfo;
    }
}
