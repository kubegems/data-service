package com.cloudminds.bigdata.dataservice.standard.manage.mapper;


import java.util.List;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.CommonField;
import org.apache.ibatis.annotations.*;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.ModelField;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.ModelInfo;
import com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler;

@Mapper
public interface ModelFieldMapper {
	@Select("select * from dev_model where type=#{type} and version=#{version} and model_name=#{model_name} and deleted=0")
	@Results(value = {
			@Result(column = "fields", property = "fields",typeHandler = JsonListTypeHandler.class)})
	public ModelField findModelFieldByVersion(int type,String version,String model_name);

	@Select("select count(*) from dev_model where type=#{type} and model_name=#{model_name} and deleted=0")
	public int countModelFieldByModelName(int type,String model_name);
	
	@Select("select * from dev_model where type=#{type} and model_name=#{model_name} and deleted=0 order by version desc LIMIT 1")
	@Results(value = {
			@Result(column = "fields", property = "fields",typeHandler = JsonListTypeHandler.class)})
	public ModelField findLastModelField(int type,String model_name);
	
	@Select("select DISTINCT model_name,model_id from dev_model where type=#{type} and deleted=0 ")
	public List<ModelInfo> findModel(int type);

	@Update("update dev_model set deleted=1 where type=#{type} and version=#{version} and model_name=#{model_name}")
	public int deleteModelField(int type, String version,String model_name);

	@Update("update dev_model set descr=#{descr}, fields=#{fields,typeHandler=com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler} where type=#{type} and version=#{version} and model_name=#{model_name}")
	public int updateModelField(ModelField modelField);

	@Insert("insert into dev_model(type, version,model_id, model_name,fields,creator,descr,create_time,update_time) "
			+ "values(#{type}, #{version},#{model_id}, #{model_name}, #{fields,typeHandler=com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler}, #{creator},#{descr},now(),now())")
	public int insertModelField(ModelField modelField) throws Exception;

	@Select("select * from dev_model where deleted=0 ${condition} limit #{startLine},#{size}")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public List<ModelField> queryModelField(String condition, int startLine, int size);

	@Select("select count(*) from dev_model where deleted=0 ${condition}")
	public int queryModelFieldCount(String condition);

	@Select("select max(model_id) from dev_model")
	public String findMaxModelId();
}
