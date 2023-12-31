package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.MetaDataSource;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.MetaDataTable;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.MetaDataTableExtendInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.handler.ArrayVarcharHandlerSpecial;
import com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface MetaDataTableMapper {
    @Select("select m.*,t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from (select * from metadata_table where deleted=0 and database_name=#{database_name} and name=#{name} and table_type=#{table_type}) m left join theme t on m.theme_id=t.id left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id")
    @Results({
            @Result(column = "columns", property = "columns", typeHandler = JsonListTypeHandler.class),
            @Result(column = "partition_field", property = "partition_field", typeHandler = JsonListTypeHandler.class),
            @Result(column = "order_field", property = "order_field", typeHandler = ArrayVarcharHandlerSpecial.class)
    })
    public MetaDataTableExtendInfo findMetaDataTableByName(String database_name, String name, int table_type);

    @Select("select * from metadata_table where deleted=0 and id=#{id}")
    @Results({
            @Result(column = "columns", property = "columns", typeHandler = JsonListTypeHandler.class),
            @Result(column = "partition_field", property = "partition_field", typeHandler = JsonListTypeHandler.class),
            @Result(column = "order_field", property = "order_field", typeHandler = ArrayVarcharHandlerSpecial.class)
    })
    public MetaDataTable findMetaDataTableById(int id);

    @Update("update metadata_table set deleted=null where id=#{id}")
    public int deleteMetaDataTableById(int id);

    @Insert("insert into metadata_table(database_name,name,table_type,storage_format,external_table,system_storage_location,storage_location,system_delimiter,delimiter,`partition`,model_level,life_cycle,data_domain,theme_id,ddl,columns,partition_field,order_field,creator,descr,mapping_instance_table,create_time,update_time) "
            + "values(#{database_name},#{name},#{table_type},#{storage_format},#{external_table},#{system_storage_location},#{storage_location},#{system_delimiter},#{delimiter},#{partition},#{model_level},#{life_cycle},#{data_domain},#{theme_id},#{ddl},#{columns,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler},#{partition_field,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler},#{order_field,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.ArrayVarcharHandlerSpecial},#{creator},#{descr},#{mapping_instance_table},now(),now())")
    public int insertMetaDataTable(MetaDataTable metaDataTable);

    @Insert("insert into metadata_table(database_name,name,table_type,storage_format,external_table,system_storage_location,storage_location,system_delimiter,delimiter,`partition`,model_level,life_cycle,data_domain,theme_id,ddl,columns,partition_field,creator,descr,create_time,update_time) "
            + "values(#{database_name},#{name},#{table_type},#{storage_format},#{external_table},#{system_storage_location},#{storage_location},#{system_delimiter},#{delimiter},#{partition},#{model_level},#{life_cycle},#{data_domain},#{theme_id},#{ddl},#{columns,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler},#{partition_field,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler},#{creator},#{descr},#{create_time},now())")
    public int insertMetaDataTableHaveCreateTime(MetaDataTable metaDataTable);

    @Insert("insert into metadata_table(database_name,name,table_type,storage_format,`partition`,life_cycle,data_domain,theme_id,ddl,columns,partition_field,order_field,creator,descr,create_time,update_time,mapping_instance_table) "
            + "values(#{database_name},#{name},#{table_type},#{storage_format},#{partition},#{life_cycle},#{data_domain},#{theme_id},#{ddl},#{columns,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler},#{partition_field,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler},#{order_field,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.ArrayVarcharHandlerSpecial},#{creator},#{descr},#{create_time},#{update_time},#{mapping_instance_table})")
    public int insertMetaDataTableHistoryCk(MetaDataTable metaDataTable);

    @Update("update metadata_table set name=#{name},model_level=#{model_level},life_cycle=#{life_cycle},data_domain=#{data_domain},theme_id=#{theme_id},ddl=#{ddl},columns=#{columns,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.JsonListTypeHandler},descr=#{descr} where id=#{id}")
    public int updateMetaDataTable(MetaDataTable metaDataTable);

    @Select("SELECT m.*,tt.* from metadata_table m LEFT JOIN (select t.id, t.name as theme_name,b.`name` as business_name_three_level,b.id as business_id_three_level,bb.id as business_id_two_level,bb.`name` as business_name_two_level,bbb.id as business_id_one_level,bbb.`name` as business_name_one_level from theme t left join business b on t.business_id=b.id left join business bb on b.pid=bb.id left join business bbb on bb.pid = bbb.id) as tt on m.theme_id=tt.id where ${condition}")
    @Results({
            @Result(column = "columns", property = "columns", typeHandler = JsonListTypeHandler.class),
            @Result(column = "partition_field", property = "partition_field", typeHandler = JsonListTypeHandler.class),
            @Result(column = "order_field", property = "order_field", typeHandler = ArrayVarcharHandlerSpecial.class)
    })
    public List<MetaDataTableExtendInfo> findMetaDataTable(String condition);

    @Select("select * from metadata_source where table_type_id=#{table_type_id} and deleted=0")
    public MetaDataSource findMetaDataSource(int table_type_id);
}
