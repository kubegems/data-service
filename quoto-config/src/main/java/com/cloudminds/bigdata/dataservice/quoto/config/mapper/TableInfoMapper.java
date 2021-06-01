package com.cloudminds.bigdata.dataservice.quoto.config.mapper;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableInfo;


@Mapper
public interface TableInfoMapper {
	@Select("SELECT * FROM Table_info WHERE is_delete=0 AND database_id=#{dataBaseId}")
	public List<TableInfo> getTableInfoByDataBaseId(int dataBaseId);

	@Update("update Table_info set state=#{state} where id=#{id}")
	public int updateTableInfoStatus(int id, int state);
	
	@Select("select SUM(num) from ((select count(*) as num from Column_alias where table_id=#{id} and is_delete=0) union (select count(*) as num from Quoto_info where table_id=#{id} and is_delete=0)) as tt")
	public int relateQuotoOrColumnNum(int id);

	@Update("update Table_info set is_delete=#{delete} where id=#{id}")
	public int updateTableInfoDelete(int id, int delete);
	
	@Update("update Table_info set table_alias=#{table_alias},des=#{des},table_name=#{table_name},is_delete=0,state=1 where id=#{id}")
	public int updateTableInfo(TableInfo tableInfo);

	@Select("SELECT * FROM Table_info WHERE table_name=#{table_name} AND database_id=#{database_id} ")
	public TableInfo getTableInfo(TableInfo tableInfo);

	@Update("insert into Table_info(table_name,database_id,table_alias,des) VALUES(#{table_name},#{database_id},#{table_alias},#{des})")
	public int insertTableInfo(TableInfo tableInfo);
}
