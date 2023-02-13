package com.cloudminds.bigdata.dataservice.quoto.search.mapper;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.DataSet;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.DbInfo;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.Directory;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.ServicePathInfo;
import com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler;
import com.cloudminds.bigdata.dataservice.quoto.search.handler.JsonListTypeHandler;
import org.apache.ibatis.annotations.*;
import org.apache.ibatis.type.JdbcType;

import java.sql.Array;
import java.util.List;

@Mapper
public interface DataSetMapper {
    @Select("select * from directory where deleted=0 and id=#{id}")
    Directory findDirectoryById(int id);

    @Select("select * from directory where deleted=0 and name=#{name} and creator=#{creator} and pid=#{pid}")
    Directory findDirectoryByNameAndCreator(String name, String creator, int pid);

    @Insert("insert into directory(name,pid,creator,descr,create_time,update_time) "
            + "values(#{name},#{pid},#{creator},#{descr},now(),now())")
    @Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
    int addDirectory(Directory directory);

    @Update("update directory set name=#{name},pid=#{pid},descr=#{descr} where id=#{id}")
    int updateDirectory(Directory directory);

    @Update("update directory set deleted=null where id=#{id}")
    int deleteDirectory(int id);

    @Select("select * from directory where deleted=0 and pid=#{pid}")
    List<Directory> findDirectoryByPid(int pid);

    @Select("select * from directory where ${condition}")
    List<Directory> queryDirectory(String condition);

    @Select("select * from data_set where deleted=0 and directory_id=#{directory_id}")
    List<DataSet> findDataSetByDirectoryId(int directory_id);

    @Select("select * from data_set where deleted=0 and name=#{name} and creator=#{creator} and directory_id=#{directory_id}")
    DataSet findDataSetByByNameAndCreator(String name, String creator, int directory_id);

    @Select("select * from data_set where deleted=0 and id=#{id}")
    @Result(column = "tag_enum_values", property = "tag_enum_values", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    @Result(column = "tag_item_complexs", property = "tag_item_complexs", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    @Result(column = "data_columns", property = "data_columns", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = JsonListTypeHandler.class)
    DataSet findDataSetByById(int id);

    @Select("select * from data_set where ${condition}")
    @Result(column = "tag_enum_values", property = "tag_enum_values", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    @Result(column = "tag_item_complexs", property = "tag_item_complexs", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    @Result(column = "data_columns", property = "data_columns", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = JsonListTypeHandler.class)
    List<DataSet> queryAllDataSet(String condition);

    @Select("select * from data_set where ${condition} limit #{startLine},#{size}")
    @Result(column = "tag_enum_values", property = "tag_enum_values", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    @Result(column = "tag_item_complexs", property = "tag_item_complexs", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    @Result(column = "data_columns", property = "data_columns", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = JsonListTypeHandler.class)
    List<DataSet> queryDataSet(String condition,int startLine,int size);

    @Select("select count(*) from data_set where ${condition}")
    int queryDataSetCount(String condition);

    @Insert("insert into data_set(name,mapping_ck_table,data_type,data_source_id,data_source_name,data_source_type,data_connect_type,directory_id,data_rule,data_columns,tag_item_complexs,tag_enum_values,creator,descr,create_time,update_time,state,message) "
            + "values(#{name},#{mapping_ck_table},#{data_type},#{data_source_id},#{data_source_name},#{data_source_type},#{data_connect_type},#{directory_id},#{data_rule},#{data_columns,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.JsonListTypeHandler},#{tag_item_complexs,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler},#{tag_enum_values,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler},#{creator},#{descr},now(),now(),#{state},#{message})")
    @Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
    int addDataSet(DataSet dataSet);

    @Update("update data_set set name=#{name},data_type=#{data_type},data_source_id=#{data_source_id},data_source_name=#{data_source_name},data_source_type=#{data_source_type},directory_id=#{directory_id},data_rule=#{data_rule},data_columns=#{data_columns,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.JsonListTypeHandler},tag_item_complexs=#{tag_item_complexs,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler},tag_enum_values=#{tag_enum_values,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler},descr=#{descr} where id=#{id}")
    int updateDataSet(DataSet dataSet);

    @Update("update data_set set state=#{state},message=#{message} where id=#{id}")
    int updateDataSetState(int state,String message,int id);

    @Update("update data_set set data_rule=#{data_rule} where id=#{id}")
    int updateDataSetDataRule(int id,String data_rule);

    @Update("update data_set set deleted=null where id=#{id}")
    int deleteDataSet(int id);

    @Select("select t.table_alias as tableName,CONCAT(db.service_path,d.service_path) as path,d.database as dbName from Table_info t LEFT JOIN Database_info d ON t.database_id=d.id LEFT JOIN Db_info db ON d.db_id=db.id where t.id=#{id}")
    ServicePathInfo queryServicePathInfo(int tableId);

    @Select("select dd.* from (select * from Table_info where id=#{tableId} and is_delete=0) t LEFT JOIN Database_info d on t.database_id=d.id left join Db_info dd on d.db_id=dd.id")
    DbInfo queryDbInfo(int tableId);
}
