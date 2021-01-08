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

	@Update("update table_info set state=#{state} where id=#{id}")
	public int updateTableInfoStatus(int id, int state);

	@Update("update table_info set is_delete=#{delete} where id=#{id}")
	public int updateTableInfoDelete(int id, int delete);
	
	@Update("update table_info set table_alias=#{table_alias} where id=#{id}")
	public int updateTableInfoAlias(int id, String table_alias);

	@Select("SELECT * FROM Table_info WHERE table=#{table} AND database_id=#{database_id} ")
	public TableInfo getTableInfo(TableInfo tableInfo);

	@Update("insert into Table_info(table,database_id,table_alias) VALUES(#{table},#{database_id},#{table_alias})")
	public int insertTableInfo(TableInfo tableInfo);
}
