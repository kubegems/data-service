package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagEnumValue;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItem;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.TagInfo;
import org.apache.ibatis.annotations.*;

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

    @Select("select * from tag_item where tag_cate_id=#{tag_cate_id} and tag_name=#{tag_name} and deleted=0")
    public TagItem findTagItemByTagCateIdAndName(String tag_cate_id, String tag_name);

    @Select("select max(right(tag_id,3)) from tag_item where tag_cate_id=#{tag_cate_id}")
    public String findMaxTagIdCode(String tag_cate_id);

    @Insert("insert into tag_item(tag_id,tag_name,tag_cate_id,value_type,value_scope,source,tag_type,tag_rule,exclusive,update_cycle,creator,updater,descr) " +
            "values(#{tag_id},#{tag_name},#{tag_cate_id},#{value_type},#{value_scope},#{source},#{tag_type},#{tag_rule},#{exclusive},#{update_cycle},#{creator},#{creator},#{descr})")
    public int insertTagItem(TagItem tagItem);

    @Update("update tag_item set tag_name=#{tag_name},value_type=#{value_type},value_scope=#{value_scope},source=#{source},tag_rule=#{tag_rule},update_cycle=#{update_cycle},updater=#{updater},descr=#{descr} where tag_id=#{tag_id}")
    public int updateTagItem(TagItem tagItem);

    @Insert({ "<script>", "insert into tag_enum_value(tag_enum_id,tag_value, tag_id,creator,updater,descr) values ",
            "<foreach collection='enum_value' item='item' index='index' separator=','>", "(#{item.tag_enum_id}, #{item.tag_value},#{tag_id},#{creator},#{creator},#{item.descr})",
            "</foreach>", "</script>" })
    public int batchSaveTagEnumValue(List<TagEnumValue> enum_value, String tag_id, String creator);

    @Update("<script><foreach collection = 'enum_value' item ='item' open='' close='' separator=';'>update tag_enum_value set tag_value =#{item.tag_value},updater=#{updater},descr=#{item.descr} where tag_enum_id =#{item.tag_enum_id}</foreach></script>")
    public int batchUpdateTagEnumValue(List<TagEnumValue> enum_value,String updater);

    @Update("update tag_enum_value set deleted=null where tag_id=#{tag_id}")
    public int deleteTagEnumValueByTagId(String tag_id);

    @Update({"<script> update tag_enum_value set deleted=null,updater=#{updater} "+
            "where  deleted=0 and tag_id=#{tag_id} and tag_enum_id not in " +
                    "<foreach collection ='enum_value' item ='items' index ='index' separator=',' open='(' close=')'  > " +
                    "#{items.tag_enum_id} " +
                    "</foreach> </script>" })
    public int deleteTagEnumValue(List<TagEnumValue> enum_value,String tag_id,String updater);

    @Update({"<script> update tag_item set state=#{state}, updater=#{updater} "+
            "where  deleted=0 and state!=#{state} and tag_id in " +
            "<foreach collection ='tag_ids' item ='items' index ='index' separator=',' open='(' close=')'  > " +
            "#{items} " +
            "</foreach> </script>" })
    public int updateTagItemState(String[] tag_ids,int state,String updater);

    @Select({"<script> select GROUP_CONCAT(tag_name) from tag_item where state=1 and tag_id in <foreach collection ='tag_ids' item ='items' index ='index' separator=',' open='(' close=')'  > " +
            "#{items} </foreach> </script>"})
    public String findOnlineTagItemName(String[] tag_ids);

    @Update({"<script> update tag_item set deleted=null, updater=#{updater} "+
            "where deleted=0 and tag_id in " +
            "<foreach collection ='tag_ids' item ='items' index ='index' separator=',' open='(' close=')'  > " +
            "#{items} " +
            "</foreach> </script>" })
    public int batchDeleteTagItem(String[] tag_ids,String updater);

    @Select("select max(right(tag_enum_id,3)) from tag_enum_value where tag_id=#{tag_id}")
    public String findMaxTagEnumIdCode(String tag_id);

    @Select("select i.* from tag_item i LEFT JOIN tag_cate c on i.tag_cate_id=c.tag_cate_id where i.deleted=0 and c.tag_object_id=#{tag_object_id}")
    @Results({
            @Result(property = "tag_id", column = "tag_id"),
            @Result(property = "tagEnumValueList", column = "tag_id", javaType = List.class,
                    many = @Many(select = "com.cloudminds.bigdata.dataservice.label.manage.mapper.LabelItemMapper.findTagEnumValueByTagId")) })
    public List<TagItem> findTagItemByTagObjectId(int tag_object_id);

    @Select("select * from tag_enum_value where deleted=0 and tag_id=#{tag_id}")
    public List<TagEnumValue> findTagEnumValueByTagId(String tag_id);
}
