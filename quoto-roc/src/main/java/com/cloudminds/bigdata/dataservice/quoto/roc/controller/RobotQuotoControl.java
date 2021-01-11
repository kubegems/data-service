package com.cloudminds.bigdata.dataservice.quoto.roc.controller;

import javax.servlet.http.HttpSession;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import apijson.framework.APIJSONController;
import apijson.framework.APIJSONParser;
import apijson.orm.Parser;

@RestController
@RequestMapping("/roc/quoto")
public class RobotQuotoControl extends APIJSONController {	
	@Override
	public Parser<Long> newParser(HttpSession session,apijson.RequestMethod method) {
		return super.newParser(session, method).setNeedVerify(false); // TODO 这里关闭校验，方便新手快速测试，实际线上项目建议开启
	}

	@PostMapping(value = "get")
	@Override
	public String get(@RequestBody String request, HttpSession session) {
		return super.get(request, session);
	}
	
	@GetMapping(value="refreshConfig")
	public void refush() {
		APIJSONParser abstractParser=new APIJSONParser();
		abstractParser.loadAliasConfig();
	}

}
