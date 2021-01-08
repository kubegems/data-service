package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableAlias;


@Mapper
public interface TableAliasMapper {
	@Select("SELECT * FROM Table_alias WHERE is_delete=0 AND table_id=#{tableId}")
	public List<TableAlias> getTableAliasByTableId(int tableId);

	@Update("update Table_alias set state=#{state} where id=#{id}")
	public int updateTableAliasStatus(int id, int state);

	@Update("update Table_alias set is_delete=#{delete} where id=#{id}")
	public int updateTableAliasDelete(int id,int delete);
	
	@Select("SELECT * FROM Table_alias WHERE table_alias=#{table_alias} AND table_id=#{table_id}")
	public TableAlias getTableAlias(TableAlias tableAlias);
	
	@Update("insert into Table_alias(table_id,table_alias) VALUES(#{table_id},#{table_alias})")
	public int insertTableAlias(TableAlias tableAlias);
}
