package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.BaseEntity;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

@EqualsAndHashCode(callSuper = false)
@Data
public class DataSetTask extends BaseEntity {
    private int id;
    private String name;
    private int data_set_id;
    private int type;
    private int import_type;
    private int export_type;
    private String export_parameters;
    private int sync_type;
    private String advanced_parameters;
    private String cron;
    private String condition;
    private String run_info;
    private String oozie_hue_uuid;
    private String workflow_hue_uuid;
    private String waterdrop_hue_uuid;
    private int state;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date start_time;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date end_time;
}
