package com.cloudminds.bigdata.dataservice.quoto.roc.controller;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import apijson.JSON;
import com.cloudminds.bigdata.dataservice.quoto.roc.service.SaveAccessHistory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.roc.redis.RedisUtil;

import apijson.framework.APIJSONController;
import apijson.orm.AbstractSQLConfig;
import apijson.orm.Parser;

@RestController
@RequestMapping("/roc/unForce/quoto")
public class RobotQuotoUnforceControl extends APIJSONController {
    @Autowired
    private RedisUtil redisUtil;
    String serviceName = "roc";
    @Autowired
    private SaveAccessHistory saveAccessHistory;

    @Override
    public Parser<Long> newParser(HttpSession session, apijson.RequestMethod method) {
        return super.newParser(session, method).setNeedVerify(false); // TODO 这里关闭校验，方便新手快速测试，实际线上项目建议开启
    }

    @PostMapping(value = "get")
    public String getHarixDataNoForce(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "get");
    }

    @PostMapping(value = "cephMeta")
    public String getCephMetaDataNoForce(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'ceph_meta'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "cephMeta");
    }

    @PostMapping(value = "sv")
    public String getSvData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'sv'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "sv");
    }

    @PostMapping(value = "roc")
    public String getRocData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'roc'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "roc");
    }

    @PostMapping(value = "vbn")
    public String getVbnData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'vbn'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "vbn");
    }

    @PostMapping(value = "maQiaoDb")
    public String getMaqiaodbData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'maqiaodb'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "maQiaoDb");
    }

    @PostMapping(value = "menJing")
    public String getMenjingData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'menjing'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "menJing");
    }

    @PostMapping(value = "fangCangDb")
    public String getFangcangdbData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'fangcangdb'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "fangCangDb");
    }

    @PostMapping(value = "cross")
    public String getCrossData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'cross'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "cross");
    }

    @PostMapping(value = "cropsRedash")
    public String getcropsRedashData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'crops_redash'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "cropsRedash");
    }

    @PostMapping(value = "cms")
    public String getCmsData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'cms'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "cms");
    }

    @PostMapping(value = "boss")
    public String getBossData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'boss'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "boss");
    }

    @PostMapping(value = "cdmCo")
    public String getCdmCoDataNoForce(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'cdm_co'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "cdmCo");
    }

    @PostMapping(value = "cmd")
    public String getCmdData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'cmd'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session, "cmd");
    }
    @PostMapping(value = "cloud")
    public String getCloudData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'cloud'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"cloud");
    }

    @PostMapping(value = "omd")
    public String getOmdData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@force':false,'@schema':'omd'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"omd");
    }

    public String getData(String request, HttpServletRequest httpServletRequest, String servicePath) {
        JSONObject response=new JSONObject();
        response.put("ok",false);
        response.put("code",401);
        //取表名
        String tableName="";
        try {
            tableName = JSON.parseObject(JSON.parseObject(request).get("[]")).keySet().iterator().next();
        }catch (Exception e){
            response.put("msg","验证用户权限时解析表名出错,请检查请求参数是否合法!");
            return response.toString();
        }

        HttpSession session = httpServletRequest.getSession();
        String token = httpServletRequest.getHeader("token");
        //第一步取token
        if (token == null) {
            response.put("msg","token不能为空!");
            return response.toString();
        }
        //第二步验证token值对应的权限
        Object token_map_object=redisUtil.get(serviceName+"_token");
        if(token_map_object!=null){
            Map<String, String> token_map = JSONObject.parseObject(JSONObject.toJSONString(token_map_object),
                    Map.class);
            if(token_map!=null){
                String tokenAccess=token_map.get(token);
                boolean hasAccess = false;
                if(tokenAccess==null){
                    response.put("msg","用户没有此表的访问权限,请联系管理员!");
                    return response.toString();
                }
                if (!tokenAccess.equals("ALL")) {
                    String[] tokenAccessList = tokenAccess.toString().split(",");
                    for (String tokenAccessValue : tokenAccessList) {
                        if (tokenAccessValue.equals(servicePath + "." + tableName)) {
                            hasAccess = true;
                            break;
                        }
                    }
                    if (!hasAccess) {
                        response.put("msg","用户没有"+tableName+"表的访问权限,请联系管理员!");
                        return response.toString();
                    }
                }
            }
        }

        // 从redis获取配置信息
        try {
            Object TABLE_KEY_MAP = redisUtil.get(serviceName + "_table_key_map");
            Object TABLE_COLUMN_MAP = redisUtil.get(serviceName + "_table_column_map");
            if (TABLE_KEY_MAP != null) {
                @SuppressWarnings("unchecked")
                Map<String, String> table_key_map = JSONObject.parseObject(JSONObject.toJSONString(TABLE_KEY_MAP),
                        Map.class);
                if (table_key_map != null) {
                    AbstractSQLConfig.TABLE_KEY_MAP.clear();
                    AbstractSQLConfig.TABLE_KEY_MAP = table_key_map;
                }
            }
            if (TABLE_COLUMN_MAP != null) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, String>> table_column_map = JSONObject
                        .parseObject(JSONObject.toJSONString(TABLE_COLUMN_MAP), Map.class);
                if (table_column_map != null) {
                    AbstractSQLConfig.tableColumnMap.clear();
                    AbstractSQLConfig.tableColumnMap = table_column_map;
                }
            }

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        // 从redis获取查询数据
        if (request == null || request.equals("")) {
            return get(request, session);
        }
        String item = DigestUtils.md5DigestAsHex(request.getBytes(StandardCharsets.UTF_8));
        Object value = null;
        boolean redisExce = false;
        try {
            value = redisUtil.hget(serviceName, item);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            redisExce = true;
        }
        if (value != null) {
            String valueS = value.toString();
            if (!valueS.equals("")) {
                accessHistory(token,servicePath,tableName,valueS,session);
                JSONObject jsonResult=JSON.parseObject(valueS);
                jsonResult.remove("execute_sql");
                return jsonResult.toString();
            }

        }
        String result = get(request, session);
        accessHistory(token,servicePath,tableName,result,session);
        if (redisExce) {
            JSONObject jsonResult=JSON.parseObject(result);
            jsonResult.remove("execute_sql");
            return jsonResult.toString();
        }
        if (result.contains("\"code\":200,\"msg\":\"success\"")) {
            if (!redisUtil.hset(serviceName, item, result, 60)) {
                System.err.println(
                        "\n\n\n redis数据存储失败,存储的value:" + result + "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
            }
        }
        JSONObject jsonResult=JSON.parseObject(result);
        jsonResult.remove("execute_sql");
        return jsonResult.toString();
    }
    public void accessHistory(String token,String service_path,String table_alias,String response, HttpSession session){
        saveAccessHistory.saveAccessHistory(token,service_path,table_alias,response,session);
        //Thread t = new Thread(new SaveAccessHistory(token,service_path,table_alias,response,session));
        //t.start();
    }
}
