package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.ColumnAlias;


@Mapper
public interface ColumnAliasMapper {
	@Select("SELECT * FROM Column_alias WHERE is_delete=0 AND table_id=#{tableId}")
	public List<ColumnAlias> getColumnAliasByTableId(int tableId);

	@Update("update Column_alias set state=#{state} where id=#{id}")
	public int updateColumnAliasStatus(int id, int state);

	@Update("update Column_alias set is_delete=#{delete} where id=#{id}")
	public int updateColumnAliasDelete(int id,int delete);
	
	@Update("update Column_alias set column_alias=#{column_alias},des=#{des},column=#{column},is_delete=0,state=1 where id=#{id}")
	public int updateColumnAlias(ColumnAlias columnAlias);

	@Select("SELECT * FROM Column_alias WHERE column=#{column} AND table_id=#{table_id}")
	public ColumnAlias getColumnAlias(ColumnAlias columnAlias);
	
	@Update("insert into Column_alias(table_id,column,column_alias,des) VALUES(#{table_id},#{column},#{column_alias},#{des})")
	public int insertColumnAlias(ColumnAlias columnAlias);
}
