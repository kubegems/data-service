package com.cloudminds.bigdata.dataservice.quoto.manage.mapper;

import java.sql.Array;
import java.util.List;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.AdjectiveExtend;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.BusinessProcess;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.DimensionExtend;
import com.sun.org.apache.xpath.internal.operations.Quo;
import org.apache.ibatis.annotations.*;
import org.apache.ibatis.type.JdbcType;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler;

@Mapper
public interface QuotoMapper {

	@Select("select * from quoto where name=#{checkValue} and deleted=0")
	public Quoto findQuotoByName(String checkValue);

	@Select("select * from quoto where field=#{checkValue} and deleted=0")
	public Quoto findQuotoByField(String checkValue);

	@Select("select * from quoto where metric=#{metric} and table_id=#{table_id} and deleted=0")
	public Quoto findQuotoByMetricAndTableId(int table_id,String metric);

	@Select("select * from quoto where id=#{id} and deleted=0")
	@Result(column = "quotos", property = "quotos", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public Quoto findQuotoById(int id);

	@Select("select name from quoto where origin_quoto=#{originQuotoId} and deleted=0")
	public List<String> findQuotoNameByOriginQuoto(int originQuotoId);

	@Select("select * from business where deleted=0 and pid=#{pid}")
	public List<Business> queryAllBusinessByPid(int pid);

	@Select("select * from business where deleted=0")
	public List<Business> queryAllBusiness();

	@Select("select * from business where deleted=0 and name=#{name} and pid=#{pid} limit 1")
	public Business queryBusiness(String name,int pid);

	@Select("select * from business where deleted=0 and code=#{code} limit 1")
	public Business queryBusinessByCode(String code);

	@Select("select * from business where deleted=0 and id=#{id} limit 1")
	public Business queryBusinessById(int id);

	@Select("select * from business where deleted=0 and pid=#{pid} limit 1")
	public List<Business> queryBusinessByPid(int pid);

	@Update("update business set deleted=null where id=#{id}")
	public int deleteBusinessById(int id);

	@Insert("insert into business(name,pid,code,create_time,update_time, creator,descr) "
			+ "values(#{name},#{pid},#{code},now(),now(), #{creator}, #{descr})")
	@Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
	public int addBusiness(Business business);

	@Update("update business set name=#{name},code=#{code},descr=#{descr} where id=#{id}")
	public int updateBusiness(Business business);

	@Select("select * from theme where deleted=0 and business_id=#{businessId}")
	public List<Theme> queryThemeByBusinessId(int businessId);

	@Select("select t.*,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id where ${condition} limit #{startLine},#{size}")
	public List<Theme> queryTheme(String condition, int startLine, int size);

	@Select("select t.*,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id where t.deleted=0")
	public List<Theme> queryAllTheme();

	@Select("select count(*) from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id where ${condition}")
	public int queryThemeTotal(String condition);

	@Select("select * from theme where deleted=0 and id=#{id}")
	public Theme queryThemeById(int id);

	@Select("select * from theme where deleted=0 and name=#{name} limit 1")
	public Theme queryThemeByName(String name);

	@Select("select * from theme where deleted=0 and code=#{code} limit 1")
	public Theme queryThemeByCode(String code);

	@Select("select * from theme where deleted=0 and en_name=#{enName} limit 1")
	public Theme queryThemeByEnName(String enName);

	@Insert("insert into theme(name,business_id,code,en_name,create_time,update_time, creator,descr) "
			+ "values(#{name},#{business_id},#{code},#{en_name},now(),now(), #{creator}, #{descr})")
	public int addTheme(Theme theme);

	@Update("update theme set name=#{name},business_id=#{business_id},code=#{code},en_name=#{en_name},descr=#{descr} where id=#{id}")
	public int updateTheme(Theme theme);

	@Update("update theme set deleted=null where id=#{id}")
	public int deleteThemeById(int id);

	@Select("select * from business_process where deleted=0 and theme_id=#{theme_id}")
	public List<BusinessProcess> queryAllBusinessProcess(int theme_id);

	@Select("select * from business_process where deleted=0 and theme_id=#{theme_id} and name=#{name} limit 1")
	public BusinessProcess queryBusinessProcess(String name, int theme_id);

	@Select("select * from business_process where deleted=0 and id=#{id}")
	public BusinessProcess queryBusinessProcessById(int id);

	@Insert("insert into business_process(name,theme_id,create_time,update_time, creator,descr) "
			+ "values(#{name},#{theme_id},now(),now(), #{creator}, #{descr})")
	public int addBusinessProcess(BusinessProcess businessProcess);

	@Update("update business_process set deleted=null where id=#{id}")
	public int deleteBusinessProcess(int id);

	@Select("select * from Table_info where is_delete=0")
	public List<TableInfo> queryAllDataService();

	@Select("select * from Table_info where is_delete=0 and (theme_id=#{theme_id} or theme_id is null)")
	public List<TableInfo> queryAllDataServiceByThemeId(int theme_id);

	@Select("select * from Quoto_info where table_id=#{tableId} and is_delete=0 and quoto_name not in (select metric from quoto where table_id=#{tableId} and deleted=0)")
	public List<QuotoInfo> queryUsableQuotoInfoByTableId(int tableId);

	@Update("update quoto set deleted=null where id=#{id}")
	public int deleteQuotoById(int id);

	@Update("update quoto set state=#{state} where id=#{id}")
	public int updateQuotoState(int state, int id);

	@Update({
			"<script> update quoto set deleted=null where id in <foreach collection='array' item='id' index='no' open='(' separator=',' close=')'> #{id} </foreach></script>" })
	public int batchDeleteQuoto(int[] id);

	@Select("SELECT q.*,tt.*,p.name as business_process_name,#{creator} as query_creator from quoto q left join business_process p on q.business_process_id=p.id LEFT JOIN (select t.id, t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id) as tt on q.theme_id=tt.id where ${condition} limit #{startLine},#{size}")
	@Results({
			@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class),
			@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class),
			@Result(property = "id", column = "id"),
			@Result(property = "tags", column = "{quoto_id=id,creator=query_creator}", javaType = List.class,
					many = @Many(select = "com.cloudminds.bigdata.dataservice.quoto.manage.mapper.TagMapper.findTagByQuotoIdAndCreator"))})
	public List<Quoto> queryQuoto(String condition, int startLine, int size,String creator);

	@Select("SELECT count(*) from quoto q left join business_process p on q.business_process_id=p.id LEFT JOIN (select t.id, t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id) as tt on q.theme_id=tt.id where ${condition}")
	public int queryQuotoCount(String condition);

	@Select("SELECT * from quoto where deleted=0 and theme_id=#{theme_id}")
	public List<Quoto> queryQuotoByTheme(int theme_id);

	@Select("SELECT * from quoto where deleted=0 and business_process_id=#{businessProcessId}")
	public List<Quoto> queryQuotoByBusinessProcess(int businessProcessId);

	@Select("SELECT * from Table_info where is_delete=0 and theme_id=#{theme_id}")
	public List<TableInfo> queryTableByTheme(int theme_id);

	@Select("SELECT * from quoto where ${condition}")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public List<Quoto> queryAllQuoto(String condition);

	@Select("select * from quoto where ${condition}")
	public List<Quoto> queryQuotoFuzzy(String condition);

	@Insert("insert into quoto(name,time_column_id,`sql`,use_sql,field,metric,theme_id,business_process_id,quoto_level,data_type,data_unit,table_id,accumulation,dimension,adjective,quotos,state,type,origin_quoto,cycle,create_time,update_time, creator,descr,expression) "
			+ "values(#{name},#{time_column_id},#{sql},#{use_sql},#{field},#{metric},#{theme_id},#{business_process_id},#{quoto_level},#{data_type},#{data_unit},#{table_id},#{accumulation},#{dimension,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},#{adjective,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},#{quotos,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},#{state}, #{type},#{origin_quoto},#{cycle},now(),now(), #{creator}, #{descr}, #{expression})")
	public int insertQuoto(Quoto quoto);

	@Insert("insert into quoto_update_history(id,name,time_column_id,`sql`,use_sql,field,metric,theme_id,business_process_id,quoto_level,data_type,data_unit,table_id,accumulation,dimension,adjective,quotos,state,type,origin_quoto,cycle,create_time,update_time, creator,descr,expression) "
			+ "values(#{id},#{name},#{time_column_id},#{sql},#{use_sql},#{field},#{metric},#{theme_id},#{business_process_id},#{quoto_level},#{data_type},#{data_unit},#{table_id},#{accumulation},#{dimension,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},#{adjective,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},#{quotos,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},#{state}, #{type},#{origin_quoto},#{cycle},#{create_time},#{update_time}, #{creator}, #{descr}, #{expression})")
	public int insertQuotoUpdateHistory(Quoto quoto);

	@Update("update quoto set name=#{name},business_process_id=#{business_process_id},time_column_id=#{time_column_id},`sql`=#{sql},use_sql=#{use_sql},field=#{field},metric=#{metric},theme_id=#{theme_id},quoto_level=#{quoto_level},data_type=#{data_type},data_unit=#{data_unit},table_id=#{table_id},accumulation=#{accumulation},origin_quoto=#{origin_quoto},cycle=#{cycle},"
			+ "dimension=#{dimension,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},adjective=#{adjective,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},quotos=#{quotos,typeHandler=com.cloudminds.bigdata.dataservice.quoto.manage.entity.handler.ArrayTypeHandler},type=#{type},descr=#{descr},expression=#{expression} where id=#{id}")
	public int updateQuoto(Quoto quoto);

	@Select("select * from cycle")
	public List<Business> queryAllCycle();

	@Select("SELECT * from quoto q LEFT JOIN (select t.id, t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id) as tt on q.theme_id=tt.id where q.deleted=0 and q.id=#{id}")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public Quoto queryQuotoById(int id);

	@Select("SELECT * from quoto_update_history q LEFT JOIN (select t.id, t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id) as tt on q.theme_id=tt.id where q.deleted=0 and q.id=#{id} order by q.update_time desc")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public List<Quoto> queryQuotoUpdateHistoryById(int id);

	@Select("SELECT * from quoto q LEFT JOIN (select t.id, t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id) as tt on q.theme_id=tt.id where q.deleted=0 and q.name=#{name}")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public Quoto queryQuotoByName(String name);
	
	@Select("SELECT * from quoto q LEFT JOIN (select t.id, t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id) as tt on q.theme_id=tt.id where q.deleted=0 and q.field=#{field}")
	@Result(column = "dimension", property = "dimension", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	@Result(column = "adjective", property = "adjective", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
	public Quoto queryQuotoByField(String field);

	@Select("SELECT * from Quoto_info where is_delete=0 and quoto_name=#{QuotoName}")
	public QuotoInfo queryQuotoInfo(String QuotoName);

	@Select("select t.table_alias as tableName,CONCAT(db.service_path,d.service_path) as path from Table_info t LEFT JOIN Database_info d ON t.database_id=d.id LEFT JOIN Db_info db ON d.db_id=db.id where t.id=#{id}")
	public ServicePathInfo queryServicePathInfo(int tableId);

	@Select("select d.*,o.name as dimension_object_name,o.code as dimension_object_code from dimension d LEFT JOIN dimension_object o on d.dimension_object_id=o.id where d.id in (select substring_index(substring_index(a.dimension,',',b.help_topic_id+1),',',-1) as id from  quoto a join mysql.help_topic b on b.help_topic_id < (length(a.dimension) - length(replace(a.dimension,',',''))+1) where a.id=#{quotoId})")
	public List<DimensionExtend> queryDimensionByQuotoId(int quotoId);

	@Select("select adjective.*,dimension.name as dimension_name,dimension.code as dimension_code from adjective LEFT JOIN dimension on dimension_id=dimension.id where adjective.id in (select substring_index(substring_index(a.adjective,',',b.help_topic_id+1),',',-1) as id from  quoto a join mysql.help_topic b on b.help_topic_id < (length(a.adjective) - length(replace(a.adjective,',',''))+1) where a.id=#{quotoId})")
	public List<AdjectiveExtend> queryAdjective(int quotoId);

	@Select("select tt.name from (select a.name, substring_index(substring_index(a.quotos,',',b.help_topic_id+1),',',-1) as id  from  quoto a join mysql.help_topic b on b.help_topic_id < (length(a.quotos) - length(replace(a.quotos,',',''))+1) where a.deleted=0 and a.quotos!='') as tt where tt.id=#{id}")
	public List<String> findQuotoNameByContainQuotoId(int id);

}
