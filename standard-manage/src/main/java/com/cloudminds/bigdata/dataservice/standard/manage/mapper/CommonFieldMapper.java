package com.cloudminds.bigdata.dataservice.standard.manage.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.CommonField;
import com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler;

@Mapper
public interface CommonFieldMapper {
	@Select("select * from common_field where type=#{type} and version=#{version} and deleted=0")
	@Results(value = {
			@Result(column = "fields", property = "fields",typeHandler = JsonListTypeHandler.class)})
	public CommonField findCommonFieldByVersion(int type,String version);
	
	@Select("select * from common_field where type=#{type} and deleted=0 order by version desc LIMIT 1")
	@Results(value = {
			@Result(column = "fields", property = "fields",typeHandler = JsonListTypeHandler.class)})
	public CommonField findLastCommonField(int type);

}
