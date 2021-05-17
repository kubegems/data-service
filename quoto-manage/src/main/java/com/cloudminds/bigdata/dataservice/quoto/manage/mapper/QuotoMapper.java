package com.cloudminds.bigdata.dataservice.quoto.manage.mapper;

import java.sql.Array;
import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.type.JdbcType;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Business;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Dimension;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Quoto;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.QuotoInfo;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.ServicePathInfo;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.TableInfo;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler;

@Mapper
public interface QuotoMapper {

	@Select("select * from quoto where name=#{checkValue} and deleted=0")
	public Quoto findQuotoByName(String checkValue);

	@Select("select * from quoto where field=#{checkValue} and deleted=0")
	public Quoto findQuotoByField(String checkValue);
	
	@Select("select * from quoto where id=#{id} and deleted=0")
	public Quoto findQuotoById(int id);
	
	@Select("select name from quoto where origin_quoto=#{originQuotoId} and deleted=0")
	public List<String> findQuotoNameByOriginQuoto(int originQuotoId);

	@Select("select * from business where deleted=0")
	public List<Business> queryAllBusiness();

	@Select("select * from data_domain where deleted=0 and business_id=#{businessId}")
	public List<Business> queryAllDataDomain(int businessId);

	@Select("select * from business_process where deleted=0 and data_domain_id=#{dataDomainId}")
	public List<Business> queryAllBusinessProcess(int dataDomainId);

	@Select("select * from Table_info where is_delete=0")
	public List<TableInfo> queryAllDataService();

	@Select("select d.* from dimension d LEFT JOIN Column_alias c ON d.column_alias=c.column_alias where c.table_id=#{tableId} and d.deleted=0 AND c.is_delete=0")
	public List<Dimension> queryAllDimension(int tableId);

	@Update("update quoto set deleted=null where id=#{id}")
	public int deleteQuotoById(int id);
	
	@Update("update quoto set state=#{state} where id=#{id}")
	public int updateQuotoState(int state,int id);

	@Update({
			"<script> update quoto set deleted=null where id in <foreach collection='array' item='id' index='no' open='(' separator=',' close=')'> #{id} </foreach></script>" })
	public int batchDeleteQuoto(int[] id);

	@Select("SELECT * from quoto q LEFT JOIN (select b.id, b.name as business_process_name, d.name as data_domain_name,bb.name as business_name, bb.id as business_id from business_process b LEFT JOIN data_domain d on b.data_domain_id=d.id LEFT JOIN business bb on d.business_id=bb.id) as tt on q.business_process_id=tt.id where ${condition} limit #{startLine},#{size}")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public List<Quoto> queryQuoto(String condition, int startLine, int size);
	
	@Select("select count(*) from quoto q LEFT JOIN (select b.id, b.name as business_process_name, d.name as data_domain_name,bb.name as business_name, bb.id as business_id from business_process b LEFT JOIN data_domain d on b.data_domain_id=d.id LEFT JOIN business bb on d.business_id=bb.id) as tt on q.business_process_id=tt.id where ${condition}")
	public int queryQuotoCount(String condition);
	
	@Select("SELECT * from quoto where ${condition}")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public List<Quoto> queryAllQuoto(String condition);
	
	@Select("select * from quoto where ${condition}")
	public List<Quoto> queryQuotoFuzzy(String condition);

	@Insert("insert into quoto(name, field,business_process_id,quoto_level,data_type,data_unit,table_id,accumulation,dimension,adjective,state,type,origin_quoto,cycle,create_time,update_time, creator,descr) "
			+ "values(#{name}, #{field}, #{business_process_id},#{quoto_level},#{data_type},#{data_unit},#{table_id},#{accumulation},#{dimension,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},#{adjective,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},#{state}, #{type},#{origin_quoto},#{cycle},now(),now(), #{creator}, #{descr})")
	public int insertQuoto(Quoto quoto);

	@Update("update quoto set name=#{name}, field=#{field},business_process_id=#{business_process_id},quoto_level=#{quoto_level},data_type=#{data_type},data_unit=#{data_unit},table_id=#{table_id},accumulation=#{accumulation},origin_quoto=#{origin_quoto},cycle=#{cycle},"
			+ "dimension=#{dimension,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},adjective=#{adjective,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},type=#{type},descr=#{descr} where id=#{id}")
	public int updateQuoto(Quoto quoto);

	@Select("select * from cycle")
	public List<Business> queryAllCycle();
	
	@Select("SELECT * from quoto q LEFT JOIN (select b.id, b.name as business_process_name, d.name as data_domain_name,bb.name as business_name from business_process b LEFT JOIN data_domain d on b.data_domain_id=d.id LEFT JOIN business bb on d.business_id=bb.id) as tt on q.business_process_id=tt.id where q.deleted=0 and q.id=#{id}")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public Quoto queryQuotoById(int id);
	
	@Select("SELECT * from quoto q LEFT JOIN (select b.id, b.name as business_process_name, d.name as data_domain_name,bb.name as business_name from business_process b LEFT JOIN data_domain d on b.data_domain_id=d.id LEFT JOIN business bb on d.business_id=bb.id) as tt on q.business_process_id=tt.id where q.deleted=0 and q.name=#{name}")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public Quoto queryQuotoByName(String name);
	
	@Select("SELECT * from Quoto_info where is_delete=0 and quoto_name=#{QuotoName}")
	public QuotoInfo queryQuotoInfo(String QuotoName);
	
//	@Select("(SELECT id,quoto_name,state from Quoto_info where is_delete=0 and quoto_name=#{QuotoName}) UNION (SELECT id,column_alias as quoto_name,state from Column_alias where is_delete=0 and column_alias=#{QuotoName})")
//	public List<QuotoInfo> queryQuotoInfo(String QuotoName);
	
	@Select("select t.table_alias as tableName,CONCAT(db.service_path,d.service_path) as path from Table_info t LEFT JOIN Database_info d ON t.database_id=d.id LEFT JOIN Db_info db ON d.db_id=db.id where t.id=#{id}")
	public ServicePathInfo queryServicePathInfo(int tableId);
	
	@Select("select code from dimension where id in(select substring_index(substring_index(a.dimension,',',b.help_topic_id+1),',',-1) as id from  quoto a join mysql.help_topic b on b.help_topic_id < (length(a.dimension) - length(replace(a.dimension,',',''))+1) where a.id=#{quotoId})")
	public List<String> queryDimensionName(int quotoId);
	
	@Select("select adjective.code_name,dimension.code as name,adjective.type,adjective.req_parm as code from adjective LEFT JOIN adjective_type ON adjective.type=adjective_type.id LEFT JOIN dimension on adjective_type.dimension_id=dimension.id where adjective.id in (select substring_index(substring_index(a.adjective,',',b.help_topic_id+1),',',-1) as id from  quoto a join mysql.help_topic b on b.help_topic_id < (length(a.adjective) - length(replace(a.adjective,',',''))+1) where a.id=#{quotoId})")
	public List<Adjective> queryAdjective(int quotoId);

}
