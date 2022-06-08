package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItem;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.TagInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface LabelItemMapper {

    @Select("select tag_cate_id as tag_id,tag_cate_name as tag_name,0 as finale from tag_cate where pid=#{pid} and deleted=0")
    public List<TagInfo> findTagInfoFromCate(String pid);

    @Select("select * from tag_item where tag_id=#{tag_id} and deleted=0")
    public TagItem findTagItemByTagId(String tag_id);

    @Select("select tag_id,tag_name,0 as finale,value_type from tag_item where tag_cate_id=#{tag_cate_id} and deleted=0")
    public List<TagInfo> findTagInfoFromTagItem(String tag_cate_id);

    @Select("select tag_id,tag_value as tag_name,1 as finale,1 as value_type from tag_enum_value where tag_id=#{tag_id} and deleted=0")
    public List<TagInfo> findTagInfoFromTagEnumValue(String tag_id);
}
