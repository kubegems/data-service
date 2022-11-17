package com.cloudminds.bigdata.dataservice.quoto.manage.mapper;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.QuotoTag;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Tag;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.TagExtendInfo;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface TagMapper {

    @Select("select * from tag where deleted=0 and name=#{name} and creator=#{creator}")
    Tag findTagByNameAndCreator(String name, String creator);

    @Select("select * from tag where deleted=0 and id=#{id}")
    Tag findTayById(int id);

    @Insert("insert into tag(name,color,descr,creator,create_time,update_time) "
            + "values(#{name},#{color},#{descr},#{creator},now(),now())")
    int insertTag(Tag tag);

    @Update("update tag set name=#{name},color=#{color},descr=#{descr} where id=#{id}")
    int updateTag(Tag tag);

    @Update("update tag set deleted=null where id=#{id}")
    int deleteTag(int id);

    @Select("select t.*,tt.use_count from (select * from tag where deleted=0 and creator=#{creator})t left join(select tag_id,count(*) as use_count from quoto_tag where deleted=0 and creator=#{creator} group by tag_id) tt on t.id=tt.tag_id")
    List<TagExtendInfo> queryTagByCreator(String creator);

    @Select("select * from quoto_tag where deleted=0 and tag_id=#{tag_id} and quoto_id=#{quoto_id}")
    QuotoTag queryQuotoTagByTagIdAndQuotoId(int tag_id,int quoto_id);

    @Insert("insert into quoto_tag(tag_id,quoto_id,creator,create_time,update_time) "
            + "values(#{tag_id},#{quoto_id},#{creator},now(),now())")
    int insertQuotoTag(QuotoTag quotoTag);

    @Select("select * from quoto_tag where deleted=0 and id=#{id}")
    QuotoTag findQuotoTagById(int id);

    @Update("update quoto_tag set deleted=null where tag_id=#{tag_id} and quoto_id=#{quoto_id}")
    int deleteQuotoTag(int tag_id,int quoto_id);

    @Select("select t.* from (select tag_id from quoto_tag where deleted=0 and creator=#{creator} and quoto_id=#{quoto_id}) q left join tag t on q.tag_id=t.id where t.deleted=0")
    List<Tag> findTagByQuotoIdAndCreator(int quoto_id,String creator);
}
