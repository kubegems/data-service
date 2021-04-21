package com.cloudminds.bigdata.dataservice.quoto.roc.controller;

import java.nio.charset.StandardCharsets;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.quoto.roc.redis.RedisUtil;

import apijson.framework.APIJSONController;
import apijson.orm.Parser;
@RestController
@RequestMapping("/roc/unForce/quoto")
public class RobotQuotoUnforceControl extends APIJSONController{
	@Autowired
	private RedisUtil redisUtil;

	@Override
	public Parser<Long> newParser(HttpSession session, apijson.RequestMethod method) {
		return super.newParser(session, method).setNeedVerify(false); // TODO 这里关闭校验，方便新手快速测试，实际线上项目建议开启
	}
	
	@PostMapping(value = "get")
	public String getHarixDataNoForce(@RequestBody String request, HttpSession session) {
		request="{'@force':false,"+request.substring(request.indexOf("{")+1);
		return getData(request, session);
	}
	
	@PostMapping(value = "cephMeta")
	public String getCephMetaDataNoForce(@RequestBody String request, HttpSession session) {
		request="{'@force':false,'@schema':'ceph_meta',"+request.substring(request.indexOf("{")+1);
		return getData(request, session);
	}
		
	@PostMapping(value = "cdmCo")
	public String getCdmCoDataNoForce(@RequestBody String request, HttpSession session) {
		request="{'@force':false,'@schema':'cdm_co',"+request.substring(request.indexOf("{")+1);
		return getData(request, session);
	}
	
	
	public String getData(String request, HttpSession session) {
		String serviceName = "roc";
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
				return valueS;
			}
			
		}
		String result = get(request, session);
		if (redisExce) {
			return result;
		}
		if (result.contains("\"code\":200,\"msg\":\"success\"")) {
			if (!redisUtil.hset(serviceName, item, result, 60)) {
				System.err.println(
						"\n\n\n redis数据存储失败,存储的value:" + result + "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
			}
		}

		return result;
	}
}
