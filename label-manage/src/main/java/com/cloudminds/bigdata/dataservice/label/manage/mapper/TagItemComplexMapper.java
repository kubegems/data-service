package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItemComplex;
import com.cloudminds.bigdata.dataservice.label.manage.handler.ArrayTypeHandler;
import org.apache.ibatis.annotations.*;
import org.apache.ibatis.type.JdbcType;

import java.sql.Array;
import java.util.List;

@Mapper
public interface TagItemComplexMapper {

    @Select("select * from tag_item_complex where name=#{name} and tag_object_id=#{tag_object_id} and deleted=0")
    @Result(column = "tag_enum_values", property = "tag_enum_values", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    public TagItemComplex findTagItemComplexByName(String name,int tag_object_id);

    @Select("select * from tag_item_complex where id=#{id} and deleted=0")
    @Result(column = "tag_enum_values", property = "tag_enum_values", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    public TagItemComplex findTagItemComplexById(int id);

    @Insert("insert into tag_item_complex(name,tag_object_id,filter,tag_enum_values,creator,descr) " +
            "values(#{name},#{tag_object_id},#{filter},#{tag_enum_values,typeHandler=com.cloudminds.bigdata.dataservice.label.manage.handler.ArrayTypeHandler},#{creator},#{descr})")
    public int insertTagItemComplex(TagItemComplex tagItemComplex);

    @Update("update tag_item_complex set name=#{name},filter=#{filter},tag_enum_values=#{tag_enum_values,typeHandler=com.cloudminds.bigdata.dataservice.label.manage.handler.ArrayTypeHandler},descr=#{descr} where id=#{id}")
    public int updateTagItemComplex(TagItemComplex tagItemComplex);

    @Update("update tag_item_complex set deleted=null where id=#{id}")
    public int deleteTagItemComplex(int id);

    @Select("select * from tag_item_complex where deleted=0 order by update_time desc limit #{startLine},#{size}")
    public List<TagItemComplex> queryLabelItemComplex(int startLine, int size);

    @Select("select count(*) from tag_item_complex where deleted=0")
    public int queryLabelItemComplexCount();

    @Select("select distinct t.tag_name from (select * from tag_enum_value where tag_enum_id in ${tag_enum_values})m left join tag_item t on m.tag_id=t.tag_id where t.state!=1")
    public List<String> queryUnOnlineLableItem(String tag_enum_values);

    @Update("update tag_item_complex set state=#{state} where id=#{id}")
    public int updateTagItemComplexState(int id,int state);
}
