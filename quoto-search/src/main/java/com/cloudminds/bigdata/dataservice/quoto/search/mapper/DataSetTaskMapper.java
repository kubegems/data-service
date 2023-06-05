package com.cloudminds.bigdata.dataservice.quoto.search.mapper;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.DataSet;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.DataSetTask;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface DataSetTaskMapper {
    @Select("select * from data_set_task where deleted=0 and data_set_id=#{data_set_id}")
    List<DataSetTask> findDataSetTaskByDatasetId(int data_set_id);

    @Select("select * from data_set_task where deleted=0 and id=#{id}")
    DataSetTask findDataSetTaskById(int id);

    @Update("update data_set_task set deleted=null where id=#{id}")
    int deleteDataSetTaskById(int id);

    @Insert("insert into data_set_task(name,data_set_id,type,import_type,sync_type,`condition`,cron,advanced_parameters,oozie_hue_uuid,workflow_hue_uuid,waterdrop_hue_uuid,start_time,end_time,create_time,update_time,creator,descr,export_type,export_parameters) "
            + "values(#{name},#{data_set_id},#{type},#{import_type},#{sync_type},#{condition},#{cron},#{advanced_parameters},#{oozie_hue_uuid},#{workflow_hue_uuid},#{waterdrop_hue_uuid},#{start_time},#{end_time},now(),now(),#{creator},#{descr},#{export_type},#{export_parameters})")
    @Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
    int addDataSetTask(DataSetTask dataSetTask);

    @Update("update data_set_task set name=#{name},data_set_id=#{data_set_id},type=#{type},import_type=#{import_type},export_parameters=#{export_parameters},export_type=#{export_type},sync_type=#{sync_type},`condition`=#{condition},cron=#{cron},advanced_parameters=#{advanced_parameters},oozie_hue_uuid=#{oozie_hue_uuid},workflow_hue_uuid=#{workflow_hue_uuid},waterdrop_hue_uuid=#{waterdrop_hue_uuid},start_time=#{start_time},end_time=#{end_time},descr=#{descr} where id=#{id}")
    int updateDataSetTask(DataSetTask dataSetTask);

    @Update("update data_set_task set state=#{state},run_info=#{run_info} where id=#{id}")
    int updateTaskState(int id, int state, String run_info);
}
