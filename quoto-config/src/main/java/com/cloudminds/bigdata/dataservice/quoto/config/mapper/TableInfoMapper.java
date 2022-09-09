package com.cloudminds.bigdata.dataservice.quoto.config.mapper;
import java.util.List;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableAccessTop;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableAccessTotalByDay;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableExtendInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableInfo;


@Mapper
public interface TableInfoMapper {
	@Select("SELECT * FROM Table_info t LEFT JOIN (select t.id, t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id) as tt on t.theme_id=tt.id WHERE t.is_delete=0 AND database_id=#{dataBaseId}")
	public List<TableInfo> getTableInfoByDataBaseId(int dataBaseId);

	@Update("update Table_info set state=#{state} where id=#{id}")
	public int updateTableInfoStatus(int id, int state);
	
	@Select("select SUM(num) from ((select count(*) as num from Column_alias where table_id=#{id} and is_delete=0) union (select count(*) as num from Quoto_info where table_id=#{id} and is_delete=0)) as tt")
	public int relateQuotoOrColumnNum(int id);

	@Update("update Table_info set is_delete=#{delete} where id=#{id}")
	public int updateTableInfoDelete(int id, int delete);
	
	@Update("update Table_info set table_alias=#{table_alias},theme_id=#{theme_id},des=#{des},table_name=#{table_name},is_delete=0,state=1 where id=#{id}")
	public int updateTableInfo(TableInfo tableInfo);

	@Select("SELECT * FROM Table_info WHERE table_name=#{table_name} AND database_id=#{database_id} ")
	public TableInfo getTableInfo(TableInfo tableInfo);

	@Select("SELECT * FROM Table_info WHERE is_delete=0 and id=#{tableId}")
	public TableInfo getTableInfoById(int tableId);

	@Update("insert into Table_info(table_name,database_id,theme_id,table_alias,des) VALUES(#{table_name},#{database_id},#{theme_id},#{table_alias},#{des})")
	@Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
	public int insertTableInfo(TableInfo tableInfo);

	@Select("select GROUP_CONCAT(`name`) from quoto where table_id=#{tableId} and deleted=0 and state=1")
	public String getRelationQuotoName(int tableId);

	@Select("select t.*,d.`database`,d.db_id,concat(dd.service_path,d.service_path) as service_path,dd.service_name from Table_info t left join Database_info d on t.database_id=d.id left join Db_info dd on dd.id=d.db_id where t.is_delete=0 and t.state=1")
	public List<TableExtendInfo> getAllTableInfo();

	@Select("SELECT t.*,tt.*,CONCAT(i.service_path,dd.service_path) as service_path FROM Table_info t LEFT JOIN Database_info dd on t.database_id=dd.id LEFT JOIN Db_info i on dd.db_id=i.id LEFT JOIN (select d.id, d.name as theme_name,bb.name as business_name,bb.id as business_id from theme d LEFT JOIN business bb on d.business_id=bb.id) as tt on t.theme_id=tt.id WHERE t.is_delete=0 and t.state=1 AND t.theme_id=#{themeId}")
	public List<TableExtendInfo> getTableInfoByThemeId(int themeId);

	@Select("select count(*) from Table_info where is_delete=0 and state=1")
	public int getTableNum();

	@Select("select date(create_time) as date,count(*) as total from dataservice_access_history where date(create_time)>=#{startDate} and date(create_time)<=#{endDate} group by date")
	public List<TableAccessTotalByDay> getApiAccessTotalGroupByDay(String startDate, String endDate);

	@Select("select tb.des,tb.id,tb.table_alias,tb.table_name,count(*) as total from dataservice_access_history d left join Table_info tb on d.table_alias=tb.table_alias where date(create_time)>=#{startDate} and date(create_time)<=#{endDate} and tb.id is not null group by tb.des,tb.id,tb.table_alias,tb.table_name order by total desc limit #{top}")
	public List<TableAccessTop> getApiAccessTop(String startDate, String endDate, int top);

	@Select("select db.service_path from (select * from Table_info where id=#{tableId}) t LEFT JOIN Database_info d on t.database_id=d.id LEFT JOIN Db_info db on d.db_id=db.id")
	public String getDataServicePathByTableId(int tableId);
}
