package com.cloudminds.bigdata.dataservice.standard.manage.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.service.LogFieldService;

@RestController
@RequestMapping("/standard/logField")
public class LogFieldControl {
	@Autowired
	private LogFieldService logFieldService;

	// 获取通用的日志字段
	@RequestMapping(value = "common", method = RequestMethod.GET)
	public CommonResponse getCommonLogField(int type, String version) {
		return logFieldService.getCommonLogField(type, version);
	}

	// 获取模块的日志字段
	@RequestMapping(value = "model", method = RequestMethod.GET)
	public CommonResponse getModelLogField(int type, String version, String model_name) {
		return logFieldService.getModelLogField(type, version, model_name);
	}

	// 获取模块信息
	@RequestMapping(value = "modelInfo", method = RequestMethod.GET)
	public CommonResponse getModel(int type) {
		return logFieldService.getModelInfo(type);
	}
}
