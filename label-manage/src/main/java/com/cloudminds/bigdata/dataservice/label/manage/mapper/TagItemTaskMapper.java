package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItemComplex;
import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItemTask;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface TagItemTaskMapper {
    @Insert("insert into tag_item_task(name,type,tag_id,tag_rule_type,tag_rule,main_class,jar_package,advanced_parameters,cron,creator,descr) " +
            "values(#{name},#{type},#{tag_id},#{tag_rule_type},#{tag_rule},#{main_class},#{jar_package},#{advanced_parameters},#{cron},#{creator},#{descr})")
    public int insertTagItemTask(TagItemTask tagItemTask);

    @Select("select * from tag_item_task where deleted=0 and tag_id=#{tag_id}")
    public TagItemTask findTagItemTaskByTagId(String tag_id);

    @Select("select * from tag_item_task where deleted=0 and id=#{id}")
    public TagItemTask findTagItemTaskById(int id);

    @Update("update tag_item_task set name=#{name},type=#{type},tag_id=#{tag_id},tag_rule_type=#{tag_rule_type},tag_rule=#{tag_rule},main_class=#{main_class},jar_package=#{jar_package},advanced_parameters=#{advanced_parameters},cron=#{cron},descr=#{descr} where id=#{id}")
    public int updateTagItemTask(TagItemTask tagItemTask);

    @Update("update tag_item_task set deleted=null where id=#{id}")
    public int deleteTagItemTask(int id);
}
