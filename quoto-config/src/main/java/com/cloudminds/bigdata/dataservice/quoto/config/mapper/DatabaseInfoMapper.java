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
	@Select("select Database_info.id,Database_info.`database`,Database_info.des,Database_info.state,Database_info.is_delete,CONCAT(Db_info.service_path,Database_info.service_path) as service_path from Database_info LEFT JOIN Db_info ON Database_info.db_id=Db_info.id")
	public List<DatabaseInfo> getDataBase();
	
	@Select("SELECT * FROM Database_info WHERE is_delete=0 AND state=1 AND db_id=#{dbId}")
	public List<DatabaseInfo> getDataBaseByDbid(int dbId);
	
	@Update("update Database_info set state=#{state} where id=#{id}")
	public int updateDatabaseInfoStatus(int id, int state);

	@Update("update Database_info set is_delete=#{delete} where id=#{id}")
	public int updateDatabaseInfoDelete(int id,int delete);

	@Update("update Database_info set `database`=#{database},service_path=#{service_path},des=#{des} where id=#{id}")
	public int updateDataBaseInfo(DatabaseInfo databaseInfo);

	@Select("SELECT * FROM Database_info WHERE db_id=#{db_id} AND `database`=#{database}")
	public DatabaseInfo getDatabaseInfo(DatabaseInfo databaseInfo);
	
	@Update("insert into Database_info(db_id,`database`,service_path,des) VALUES(#{db_id},#{database},#{service_path},#{des})")
	public int insertDatabaseInfo(DatabaseInfo databaseInfo);
	
	//db_info
	@Select("SELECT * FROM Db_info WHERE is_delete=0 AND state=1")
	public List<DbInfo> getdbInfo();

	@Select("select * from Db_info where db_url=#{db_url} limit 1")
	public DbInfo getDbInfoByDbUrl(DbInfo dbInfo);

	@Select("select * from Db_info where id=#{id} and is_delete=0")
	public DbInfo getDbInfoById(int id);

	@Update("update Db_info set is_delete=#{delete} where id=#{id}")
	public int updateDbInfoDelete(int id,int delete);

	@Update("update Db_info set db_url=#{db_url},db_name=#{db_name},userName=#{userName},password=#{password},service_path=#{service_path},service_name=#{service_name},des=#{des} where id=#{id}")
	public int updateDbInfo(DbInfo dbInfo);

	@Update("insert into Db_info(db_url,db_name,userName,password,service_path,service_name,des) VALUES(#{db_url},#{db_name},#{userName},#{password},#{service_path},#{service_name},#{des})")
	public int insertDnInfo(DbInfo dbInfo);
	
	@Select("SELECT db_url,userName,`password`,`database`,table_name from Table_info LEFT JOIN Database_info ON Table_info.database_id=Database_info.id LEFT JOIN Db_info ON Database_info.db_id=Db_info.id where Table_info.id=#{tableId} AND Table_info.is_delete=0 AND Table_info.state=1")
	public DbConnInfo getdbConnInfoByTableId(int tableId);
}
