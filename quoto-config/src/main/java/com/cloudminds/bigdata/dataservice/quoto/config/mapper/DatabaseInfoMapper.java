package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.DatabaseInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.DbConnInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.DbInfo;



@Mapper
public interface DatabaseInfoMapper {
	@Select("SELECT * FROM Database_info WHERE is_delete=0 AND state=1")
	public List<DatabaseInfo> getDataBase();
	
	@Select("SELECT * FROM Database_info WHERE is_delete=0 AND state=1 AND db_id=#{dbId}")
	public List<DatabaseInfo> getDataBaseByDbid(int dbId);
	
	@Update("update Database_info set state=#{state} where id=#{id}")
	public int updateDatabaseInfoStatus(int id, int state);

	@Update("update Database_info set is_delete=#{delete} where id=#{id}")
	public int updateDatabaseInfoDelete(int id,int delete);

	@Select("SELECT * FROM Database_info WHERE db_url=#{db_url} AND database=#{database}")
	public DatabaseInfo getDatabaseInfo(DatabaseInfo databaseInfo);
	
	@Update("insert into Database_info(db_url,database) VALUES(#{db_url},#{database})")
	public int insertDatabaseInfo(DatabaseInfo databaseInfo);
	
	//db_info
	@Select("SELECT * FROM Db_info WHERE is_delete=0 AND state=1")
	public List<DbInfo> getdbInfo();
	
	@Select("SELECT db_url,userName,`password`,`database`,table_name from Table_info LEFT JOIN Database_info ON Table_info.database_id=Database_info.id LEFT JOIN Db_info ON Database_info.db_id=Db_info.id where Table_info.id=#{tableId} AND Table_info.is_delete=0 AND Table_info.state=1")
	public DbConnInfo getdbConnInfoByTableId(int tableId);
}
