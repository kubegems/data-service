package com.cloudminds.bigdata.dataservice.quoto.search.mapper;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.DataSet;
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
    int addDirectory(Directory directory);

    @Update("update directory set name=#{name} and pid=#{pid} and descr=#{descr} where id=#{id}")
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
    @Result(column = "columns", property = "columns", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = JsonListTypeHandler.class)
    DataSet findDataSetByById(int id);

    @Select("select * from data_set where ${condition}")
    List<Directory> queryAllDataSet(String condition);

    @Select("select * from data_set where ${condition} limit #{startLine},#{size}")
    List<Directory> queryDataSet(String condition,int startLine,int size);

    @Select("select count(*) from data_set where ${condition}")
    int queryDataSetCount(String condition);

    @Insert("insert into data_set(name,data_type,data_source_id,data_source_name,data_connect_type,directory_id,data_rule,data_columns,tag_item_complexs,tag_enum_values,creator,descr,create_time,update_time) "
            + "values(#{name},#{data_type},#{data_source_id},#{data_source_name},#{data_connect_type},#{directory_id},#{data_rule},#{data_columns,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.JsonListTypeHandler},#{tag_item_complexs,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler},#{tag_enum_values,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler},#{creator},#{descr},now(),now())")
    int addDataSet(DataSet dataSet);

    @Update("update data_set set name=#{name} and data_type=#{data_type} and data_source_id=#{data_source_id} and data_source_name=#{data_source_name} and data_connect_type=#{data_connect_type} and directory_id=#{directory_id} and data_rule=#{data_rule} and data_columns=#{data_columns,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.JsonListTypeHandler} and tag_item_complexs=#{tag_item_complexs,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler} and tag_enum_values=#{tag_enum_values,typeHandler=com.cloudminds.bigdata.dataservice.quoto.search.handler.ArrayTypeHandler} and descr=#{descr} where id=#{id}")
    int updateDataSet(DataSet dataSet);

    @Update("update data_set set deleted=null where id=#{id}")
    int deleteDataSet(int id);

    @Select("select t.table_alias as tableName,CONCAT(db.service_path,d.service_path) as path,d.name as dbName from Table_info t LEFT JOIN Database_info d ON t.database_id=d.id LEFT JOIN Db_info db ON d.db_id=db.id where t.id=#{id}")
    ServicePathInfo queryServicePathInfo(int tableId);
}
