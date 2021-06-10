package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.ApiDoc;
import com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler;

@Mapper
public interface ApiDocMapper {
	@Select("SELECT * FROM api_doc WHERE is_delete=0 AND state=1")
	@Results(value = {
			@Result(column = "parameters", property = "parameters",typeHandler = JsonListTypeHandler.class)})
	public List<ApiDoc> getApiDoc();
}
