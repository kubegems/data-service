package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagCate;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagObject;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface TagCateMapper {

    @Select("select * from tag_cate where deleted=0 and pid=#{pid} and tag_cate_name=#{name}")
    public TagCate queryTagCateByPidAndName(String pid, String name);

    @Select("select * from tag_cate where deleted=0 and pid=#{pid} and tag_object_id=#{tag_object_id}")
    public List<TagCate> queryTagCateByPid(String pid,int tag_object_id);

    @Select("select * from tag_cate where deleted=0 and tag_object_id=#{tag_object_id}")
    public List<TagCate> queryTagCateBTagObjectId(int tag_object_id);

    @Select("select * from tag_cate where deleted=0 and tag_cate_id=#{tag_cate_id}")
    public TagCate queryTagCateById(String tag_cate_id);

    @Select("select max(tag_cate_id) from tag_cate where pid=#{pid} and tag_object_id=#{tag_object_id}")
    public String queryMaxId(String pid,int tag_object_id);

    @Insert("insert into tag_cate(tag_cate_id,pid,tag_cate_name,tag_object_id,creator,descr) values(#{tag_cate_id},#{pid},#{tag_cate_name},#{tag_object_id},#{creator},#{descr})")
    public int insertTagCate(TagCate cate);

    @Update("update tag_cate set tag_cate_name=#{tag_cate_name},descr=#{descr} where tag_cate_id=#{tag_cate_id}")
    public int updateTagCateName(String tag_cate_id,String tag_cate_name,String descr);

    @Update("update tag_cate set deleted=null where tag_cate_id=#{tag_cate_id}")
    public int deleteTagCate(String tag_cate_id);
}
