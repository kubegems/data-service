package com.cloudminds.bigdata.dataservice.quoto.manage.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;

@Mapper
public interface AdjectiveMapper {

	@Update("update adjective set deleted=null where id=#{id}")
	public int deleteAdjectiveById(int id);

	@Update({
			"<script> update adjective set deleted=null where id in <foreach collection='array' item='id' index='no' open='(' separator=',' close=')'> #{id} </foreach></script>" })
	public int batchDeleteAdjective(int[] id);

	@Select("select * from adjective where name=#{checkValue} and deleted=0")
	public Adjective findAdjectiveByName(String checkValue);

	@Select("select * from adjective where code=#{checkValue} and deleted=0")
	public Adjective findAdjectiveByCode(String checkValue);

	@Select("select * from adjective where code_name=#{checkValue} and deleted=0")
	public Adjective findAdjectiveByCodeName(String checkValue);
	
	@Select("select * from adjective where id=#{id} and deleted=0")
	public Adjective findAdjectiveById(int id);

	@Select("select * from adjective LEFT JOIN (select substring_index(substring_index(a.adjective,',',b.help_topic_id+1),',',-1) as idddd,count(*) as quotoNum " + 
			"from  quoto a " + 
			"join   mysql.help_topic b on b.help_topic_id < (length(a.adjective) - length(replace(a.adjective,',',''))+1) where a.deleted=0 and a.adjective!='' group by idddd) as tt on id=tt.idddd where ${condition} limit #{startLine},#{size}")
	public List<Adjective> queryAdjective(String condition, int startLine, int size);
	
	@Select("select * from adjective where ${condition}")
	public List<Adjective> queryAllAdjective(String condition);

	@Select("select count(*) from adjective where ${condition}")
	public int queryAdjectiveCount(String condition);

	@Insert("insert into adjective(name, code,code_name,type,req_parm,create_time,update_time, creator,descr) "
			+ "values(#{name}, #{code}, #{code_name}, #{type}, #{req_parm},now(),now(), #{creator}, #{descr})")
	public int insertAdjective(Adjective adjective);

	@Update("update adjective set name=#{name},req_parm=#{req_parm}, code=#{code},code_name=#{code_name},type=#{type},descr=#{descr} where id=#{id}")
	public int updateAdjective(Adjective adjective);

	@Select("select tt.name from (select a.name, substring_index(substring_index(a.adjective,',',b.help_topic_id+1),',',-1) as id " + 
			"from  quoto a " + 
			"join   mysql.help_topic b on b.help_topic_id < (length(a.adjective) - length(replace(a.adjective,',',''))+1) where a.deleted=0 and a.adjective!='') as tt where tt.id=#{id}")
	public List<String> findQuotoNameByAdjectiveId(int id);

	@Select("select * from adjective where adjective.deleted=0 and type in(select adjective_type.id from adjective_type LEFT JOIN dimension ON adjective_type.dimension_id=dimension.id LEFT JOIN Column_alias ON dimension.column_alias=Column_alias.column_alias where Column_alias.table_id=#{tableId} and Column_alias.is_delete=0 AND dimension.deleted=0 and adjective_type.deleted=0)")
	public List<Adjective> querySupportAdjective(int tableId);
}
