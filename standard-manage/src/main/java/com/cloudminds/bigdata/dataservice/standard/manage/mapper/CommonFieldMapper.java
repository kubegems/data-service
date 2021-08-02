package com.cloudminds.bigdata.dataservice.standard.manage.mapper;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.EventInfo;
import org.apache.ibatis.annotations.*;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.CommonField;
import com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler;

import java.util.List;

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

	@Select("select count(*) from common_field where type=#{type} and deleted=0")
	public int findCommonFieldNum(int type);

	@Update("update common_field set deleted=1 where type=#{type} and version=#{version}")
	public int deleteCommonField(int type, String version);

	@Update("update common_field set descr=#{descr}, fields=#{fields,typeHandler=com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler} where type=#{type} and version=#{version}")
	public int updateCommonField(CommonField commonField);

	@Insert("insert into common_field(type, version,fields,creator,descr,create_time,update_time) "
			+ "values(#{type}, #{version}, #{fields,typeHandler=com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler}, #{creator},#{descr},now(),now())")
	public int insertCommonField(CommonField commonField) throws Exception;

	@Select("select * from common_field where deleted=0 ${condition} limit #{startLine},#{size}")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public List<CommonField> queryCommonField(String condition, int startLine, int size);

	@Select("select count(*) from common_field where deleted=0 ${condition}")
	public int queryCommonFieldCount(String condition);
}
