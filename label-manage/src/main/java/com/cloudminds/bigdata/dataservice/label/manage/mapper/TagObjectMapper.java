package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagObject;
import com.cloudminds.bigdata.dataservice.label.manage.handler.JsonListTypeHandler;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.type.JdbcType;

import java.sql.Array;
import java.util.List;

@Mapper
public interface TagObjectMapper {

    @Select("select * from tag_object where deleted=0")
    @Result(column = "columns", property = "columns", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = JsonListTypeHandler.class)
    public List<TagObject> queryAllTagObject();

    @Select("select * from tag_object where deleted=0 and id=#{id}")
    public TagObject queryTagObject(int id);

    @Select("select o.* from tag_cate c LEFT JOIN tag_object o ON c.tag_object_id=o.id where c.tag_cate_id=#{tag_cate_id} and o.deleted=0")
    public TagObject queryTagObjectByTagCateId(String tag_cate_id);

    ///@Select("select *,des as descr from bigdata_dataservice.Column_alias where is_delete=0 and table_id = (select t.id from bigdata_dataservice.Table_info t left join bigdata_dataservice.Database_info d on t.database_id=d.id where t.table_alias=#{tableName} and d.`database`=#{db})")
    //public List<ColumnAlias> queryTagObjectColunmAttribute(String db, String tableName);
}
