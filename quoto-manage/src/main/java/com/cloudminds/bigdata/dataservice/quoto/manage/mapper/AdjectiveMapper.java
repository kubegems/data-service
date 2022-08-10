package com.cloudminds.bigdata.dataservice.quoto.manage.mapper;

import java.util.List;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.AdjectiveExtend;
import com.cloudminds.bigdata.dataservice.quoto.manage.handler.JsonListTypeHandler;
import org.apache.ibatis.annotations.*;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;

@Mapper
public interface AdjectiveMapper {

	@Update("update adjective set deleted=null where id=#{id}")
	public int deleteAdjectiveById(int id);

	@Update({
			"<script> update adjective set deleted=null where id in <foreach collection='array' item='id' index='no' open='(' separator=',' close=')'> #{id} </foreach></script>" })
	public int batchDeleteAdjective(int[] id);

	@Select("select * from adjective where name=#{checkValue} and deleted=0")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public Adjective findAdjectiveByName(String checkValue);

	@Select("select * from adjective where code=#{checkValue} and deleted=0")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public Adjective findAdjectiveByCode(String checkValue);
	
	@Select("select * from adjective where id=#{id} and deleted=0")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public Adjective findAdjectiveById(int id);

	@Select("select * from adjective LEFT JOIN (select substring_index(substring_index(a.adjective,',',b.help_topic_id+1),',',-1) as idddd,count(*) as quotoNum " + 
			"from  quoto a " + 
			"join   mysql.help_topic b on b.help_topic_id < (length(a.adjective) - length(replace(a.adjective,',',''))+1) where a.deleted=0 and a.adjective!='' group by idddd) as tt on id=tt.idddd where ${condition} limit #{startLine},#{size}")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public List<Adjective> queryAdjective(String condition, int startLine, int size);
	
	@Select("select * from adjective where ${condition}")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public List<Adjective> queryAllAdjective(String condition);

	@Select("select count(*) from adjective where ${condition}")
	public int queryAdjectiveCount(String condition);

	@Insert("insert into adjective(dimension_id,fields,name,code,column_name,type,req_parm_type,req_parm,create_time,update_time, creator,descr) "
			+ "values(#{dimension_id},#{fields,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.handler.JsonListTypeHandler},#{name}, #{code}, #{column_name},#{type},#{req_parm_type},#{req_parm},now(),now(), #{creator}, #{descr})")
	public int insertAdjective(Adjective adjective);

	@Update("update adjective set dimension_id=#{dimension_id},fields=#{fields,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.handler.JsonListTypeHandler},name=#{name},code=#{code},column_name=#{column_name},type=#{type},req_parm_type=#{req_parm_type},req_parm=#{req_parm},descr=#{descr} where id=#{id}")
	public int updateAdjective(Adjective adjective);

	@Select("select tt.name from (select a.name, substring_index(substring_index(a.adjective,',',b.help_topic_id+1),',',-1) as id " + 
			"from  quoto a " + 
			"join   mysql.help_topic b on b.help_topic_id < (length(a.adjective) - length(replace(a.adjective,',',''))+1) where a.deleted=0 and a.adjective!='') as tt where tt.id=#{id}")
	public List<String> findQuotoNameByAdjectiveId(int id);

	@Select("select ad.* from (select * from Column_alias where table_id=#{tableId} and is_delete=0) c left join (select a.id,a.dimension_id,d.name as dimension_name,dd.id as dimension_object_id,dd.name as dimension_object_name,a.req_parm_type,a.req_parm,a.name,a.code,if(a.column_name is null,d.code,a.column_name) as column_name,a.type,a.deleted,a.descr,a.creator,a.create_time,a.update_time from adjective a left join dimension d on a.dimension_id=d.id left join dimension_object dd on d.dimension_object_id=dd.id where a.deleted=0 and a.type=2 and (a.column_name is not null or d.name is not null)) ad on c.column_alias=ad.column_name where ad.deleted=0")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public List<AdjectiveExtend> querySupportAdjective(int tableId);

	@Select("select a.id,a.dimension_id,dd.id as dimension_object_id,dd.name as dimension_object_name,a.req_parm_type,a.req_parm,a.name,a.code,a.column_name,a.type,a.deleted,a.descr,a.creator,a.create_time,a.update_time from adjective a left join dimension_object dd on dd.code='time' where a.deleted=0 and a.type=1")
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public List<AdjectiveExtend> queryTimeAdjective();

	@Select({
			"<script> select * from adjective where deleted=0 and type=1 and id in <foreach collection='array' item='ids' index='no' open='(' separator=',' close=')'> #{ids} </foreach></script>" })
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public List<Adjective> queryTimeAdjectiveByIds(int[] ids);

	@Select({
			"<script> select * from adjective where deleted=0 and id in <foreach collection='array' item='ids' index='no' open='(' separator=',' close=')'> #{ids} </foreach></script>" })
	@Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
	public List<Adjective> queryAdjectiveByIds(int[] ids);
}
