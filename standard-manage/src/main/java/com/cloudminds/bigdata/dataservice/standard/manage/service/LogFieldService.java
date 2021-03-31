package com.cloudminds.bigdata.dataservice.standard.manage.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacos.api.utils.StringUtils;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.CommonField;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.ModelField;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.CommonFieldMapper;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.ModelFieldMapper;

@Service
public class LogFieldService {
	@Autowired
	private CommonFieldMapper commonFieldMapper;
	
	@Autowired
	private ModelFieldMapper modelFieldMapper;

	public CommonResponse getCommonLogField(int type, String version) {
		CommonResponse commonResponse=new CommonResponse();
		CommonField commonField=null;
		if(StringUtils.isEmpty(version)) {
			//获取最新的字段
			commonField=commonFieldMapper.findLastCommonField(type);
		}else {
			//获取指定版本的字段
			commonField=commonFieldMapper.findLastCommonField(type);
		}
		if(commonField==null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("没有通用的字段,请联系管理员添加");
		}else {
			commonResponse.setData(commonField);
		}
		return commonResponse;
	}

	public CommonResponse getModelLogField(int type, String version, String model_name) {
		CommonResponse commonResponse=new CommonResponse();
		ModelField modelField=null;
		if(StringUtils.isEmpty(model_name)) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("模块名不能为空");
			return commonResponse;
		}
		if(StringUtils.isEmpty(version)) {
			//获取最新的字段
			modelField=modelFieldMapper.findLastModelField(type, model_name);
		}else {
			//获取指定版本的字段
			modelField=modelFieldMapper.findModelFieldByVersion(type, version, model_name);			
		}
		if(modelField==null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("没有通用的字段,请联系管理员添加");
		}else {
			commonResponse.setData(modelField);
		}
		return commonResponse;
	}

	public CommonResponse getModelInfo(int type) {
		CommonResponse commonResponse=new CommonResponse();
		commonResponse.setData(modelFieldMapper.findModel(type));
		return commonResponse;
	}
}
