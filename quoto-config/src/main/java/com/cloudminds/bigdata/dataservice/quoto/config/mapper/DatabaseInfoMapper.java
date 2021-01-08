package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.DatabaseInfo;



@Mapper
public interface DatabaseInfoMapper {
	@Select("SELECT * FROM Database_info WHERE is_delete=0 AND state=1")
	public List<DatabaseInfo> getDataBase();
	
	@Update("update Database_info set state=#{state} where id=#{id}")
	public int updateDatabaseInfoStatus(int id, int state);

	@Update("update Database_info set is_delete=#{delete} where id=#{id}")
	public int updateDatabaseInfoDelete(int id,int delete);

	@Select("SELECT * FROM Database_info WHERE db_url=#{db_url} AND database=#{database}")
	public DatabaseInfo getDatabaseInfo(DatabaseInfo databaseInfo);
	
	@Update("insert into Database_info(db_url,database) VALUES(#{db_url},#{database})")
	public int insertDatabaseInfo(DatabaseInfo databaseInfo);
}
