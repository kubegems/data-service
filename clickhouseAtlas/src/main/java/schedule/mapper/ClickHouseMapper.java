package schedule.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import schedule.entity.ColumnInfo;
import schedule.entity.DatabaseInfo;
import schedule.entity.TableInfo;


@Mapper
public interface ClickHouseMapper {
	@Select("select * from (SELECT database,sum(bytes_on_disk) AS diskSize,sum(data_uncompressed_bytes) AS originSize FROM system.parts WHERE (database != 'system') AND (active = 1) group by database) as tt LEFT JOIN system.databases as dd on tt.database=dd.name")
	public List<DatabaseInfo> getDatabaseInfo();
	
	@Select("SELECT * from `system`.tables where database =#{dataBaseName}")
	public List<TableInfo> getTableInfo(String dataBaseName);
	
	@Select("SELECT * from `system`.columns where database =#{dataBaseName} and `table` =#{tableName}")
	public List<ColumnInfo> getColumnInfo(String dataBaseName,String tableName);
}
