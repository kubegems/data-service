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

    @Select("select o.* from tag_cate c LEFT JOIN tag_object o ON c.tag_object_id=o.id where c.tag_cate_id=#{tag_cate_id} and o.deleted=0")
    public TagObject queryTagObjectByTagCateId(String tag_cate_id);
}
