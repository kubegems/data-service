package com.cloudminds.bigdata.dataservice.quoto.config.mapper;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableInfo;


@Mapper
public interface TableInfoMapper {
	@Select("SELECT * FROM Table_info t LEFT JOIN (select b.id, b.name as business_process_name, d.name as data_domain_name,bb.name as business_name from business_process b LEFT JOIN data_domain d on b.data_domain_id=d.id LEFT JOIN business bb on d.business_id=bb.id) as tt on t.business_process_id=tt.id WHERE t.is_delete=0 AND database_id=#{dataBaseId}")
	public List<TableInfo> getTableInfoByDataBaseId(int dataBaseId);

	@Update("update Table_info set state=#{state} where id=#{id}")
	public int updateTableInfoStatus(int id, int state);
	
	@Select("select SUM(num) from ((select count(*) as num from Column_alias where table_id=#{id} and is_delete=0) union (select count(*) as num from Quoto_info where table_id=#{id} and is_delete=0)) as tt")
	public int relateQuotoOrColumnNum(int id);

	@Update("update Table_info set is_delete=#{delete} where id=#{id}")
	public int updateTableInfoDelete(int id, int delete);
	
	@Update("update Table_info set table_alias=#{table_alias},business_process_id=#{business_process_id},des=#{des},table_name=#{table_name},is_delete=0,state=1 where id=#{id}")
	public int updateTableInfo(TableInfo tableInfo);

	@Select("SELECT * FROM Table_info WHERE table_name=#{table_name} AND database_id=#{database_id} ")
	public TableInfo getTableInfo(TableInfo tableInfo);

	@Select("SELECT * FROM Table_info WHERE is_delete=0 and id=#{tableId}")
	public TableInfo getTableInfoById(int tableId);

	@Update("insert into Table_info(table_name,database_id,business_process_id,table_alias,des) VALUES(#{table_name},#{database_id},#{business_process_id},#{table_alias},#{des})")
	@Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
	public int insertTableInfo(TableInfo tableInfo);

	@Select("select GROUP_CONCAT(`name`) from quoto where table_id=#{tableId} and deleted=0 and state=1")
	public String getRelationQuotoName(int tableId);
}
