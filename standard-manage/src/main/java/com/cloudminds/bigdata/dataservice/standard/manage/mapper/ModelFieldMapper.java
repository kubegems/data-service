package com.cloudminds.bigdata.dataservice.standard.manage.mapper;


import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.ModelField;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.ModelInfo;
import com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler;

@Mapper
public interface ModelFieldMapper {
	@Select("select * from dev_model where type=#{type} and version=#{version} and model_name=#{model_name} and deleted=0")
	@Results(value = {
			@Result(column = "fields", property = "fields",typeHandler = JsonListTypeHandler.class)})
	public ModelField findModelFieldByVersion(int type,String version,String model_name);
	
	@Select("select * from dev_model where type=#{type} and model_name=#{model_name} and deleted=0 order by version desc LIMIT 1")
	@Results(value = {
			@Result(column = "fields", property = "fields",typeHandler = JsonListTypeHandler.class)})
	public ModelField findLastModelField(int type,String model_name);
	
	@Select("select DISTINCT model_name,model_id from dev_model where type=#{type} and deleted=0 ")
	public List<ModelInfo> findModel(int type);

}
