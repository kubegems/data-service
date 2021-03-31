package com.cloudminds.bigdata.dataservice.standard.manage.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.EventInfo;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.Report;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.VersionInfo;
import com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler;

@Mapper
public interface EventMapper {

	@Insert("insert into eventment(event_code, event_name,model_name,model_version,version,type,type_version,jira_num,fields,state,create_time,update_time, creator,descr) "
			+ "values(#{event_code}, #{event_name}, #{model_name}, #{model_version}, #{version}, #{type},#{type_version}, #{jira_num},#{fields,typeHandler=com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler}, #{state},now(),now(), #{creator}, #{descr})")
	@Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
	public int insertEvent(EventInfo eventInfo) throws Exception;

	@Select("select * from eventment where event_name=#{name} and deleted=0")
	public EventInfo findEventByEventName(String name);

	@Select("select * from eventment where id!=#{id} and deleted=0 and event_code=#{event_code} and (state=3 or state=5)")
	public EventInfo findOtherPublishEventById(int id,String event_code);

	@Select("select max(event_code) from eventment")
	public String findMaxEventCode();

	@Select("select * from eventment where deleted=0 ${condition} limit #{startLine},#{size}")
	public List<EventInfo> queryEvent(String condition, int startLine, int size);

	@Select("select count(*) from eventment where deleted=0 ${condition}")
	public int queryEventCount(String condition);

	@Select("select * from eventment where id=#{id} and deleted=0")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public EventInfo queryEventById(int id);

	@Select("select id as eventId,version from eventment where deleted=0 and event_code=#{event_code} order by version desc")
	public List<VersionInfo> queryVersionInfo(String event_code);

	@Update("update eventment set deleted=1 where event_code=#{event_code}")
	public int deleteEventById(String event_code);

	@Update({
			"<script> update eventment set deleted=1 where event_code in <foreach collection='array' item='event_code' index='no' open='(' separator=',' close=')'> #{event_code} </foreach></script>" })
	public int batchDeleteEvent(String[] event_code);

	@Update("update eventment set state=#{state} where id=#{id}")
	public int updateEventState(int id, int state);

	@Update("update eventment set state=#{state} ,message=#{message} where id=#{id}")
	public int updateEventReviewResult(int id, int state, String message);

	@Select("select count(*) as num,type as title from eventment where state<5 and deleted=0 group by type")
	public List<Report> queryEventNumInfo();

	@Select("select count(*) as num,model_name as title from eventment where state<5 and deleted=0 and type=#{type} GROUP BY model_name")
	public List<Report> queryModelEventNumInfo(int type);

	@Update("update eventment set event_name=#{event_name},model_name=#{model_name},model_version=#{model_version},jira_num=#{jira_num},state=#{state},fields=#{fields,typeHandler=com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler},descr=#{descr},creator=#{creator} where id=#{id}")
	public int updateEvent(EventInfo event);
}
