package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItemComplex;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItemTask;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.TagItemTaskExtendinfo;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface TagItemTaskMapper {
    @Insert("insert into tag_item_task(name,type,tag_object_id,tag_id,tag_rule_type,tag_rule,main_class,jar_package,advanced_parameters,cron,oozie_hue_uuid,workflow_hue_uuid,start_time,end_time,creator,descr) " +
            "values(#{name},#{type},#{tag_object_id},#{tag_id},#{tag_rule_type},#{tag_rule},#{main_class},#{jar_package},#{advanced_parameters},#{cron},#{oozie_hue_uuid},#{workflow_hue_uuid},#{start_time},#{end_time},#{creator},#{descr})")
    public int insertTagItemTask(TagItemTask tagItemTask);

    @Select("select * from tag_item_task where deleted=0 and tag_id=#{tag_id}")
    public TagItemTask findTagItemTaskByTagId(String tag_id);

    @Select("select * from tag_item_task where deleted=0 and id=#{id}")
    public TagItemTask findTagItemTaskById(int id);

    @Update("update tag_item_task set name=#{name},type=#{type},tag_id=#{tag_id},tag_rule_type=#{tag_rule_type},tag_rule=#{tag_rule},main_class=#{main_class},jar_package=#{jar_package},advanced_parameters=#{advanced_parameters},cron=#{cron},oozie_hue_uuid=#{oozie_hue_uuid},workflow_hue_uuid=#{workflow_hue_uuid},start_time=#{start_time},end_time=#{end_time},descr=#{descr} where id=#{id}")
    public int updateTagItemTask(TagItemTask tagItemTask);

    @Update("update tag_item_task set state=#{state},run_info=#{run_info} where id=#{id}")
    public int updateTagItemTaskState(int id, int state, String run_info);

    @Update("update tag_item_task set deleted=null where id=#{id}")
    public int deleteTagItemTask(int id);

    @Select("select t.*,l.tag_name,l.state as tag_state from tag_item_task t left join tag_item l on t.tag_id=l.tag_id where ${condition} limit #{startLine},#{size}")
    public List<TagItemTaskExtendinfo> queryTagItemTask(String condition, int startLine, int size);

    @Select("select count(*) from tag_item_task t left join tag_item l on t.tag_id=l.tag_id where ${condition}")
    public int queryTagItemTaskCount(String condition);
}
