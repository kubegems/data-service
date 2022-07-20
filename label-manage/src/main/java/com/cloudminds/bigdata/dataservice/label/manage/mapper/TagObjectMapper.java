package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagObject;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface TagObjectMapper {

    @Select("select * from tag_object where deleted=0")
    public List<TagObject> queryAllTagObject();

    @Select("select * from tag_object where deleted=0 and id=#{id}")
    public TagObject queryTagObject(int id);
}
