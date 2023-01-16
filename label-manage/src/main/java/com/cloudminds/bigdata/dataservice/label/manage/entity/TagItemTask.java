package com.cloudminds.bigdata.dataservice.label.manage.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

@EqualsAndHashCode(callSuper = false)
@Data
public class TagItemTask extends BaseEntity{
    private int id;
    private String name;
    private int type;
    private int tag_object_id;
    private String tag_id;
    private int tag_rule_type=1;
    private String tag_rule;
    private String main_class;
    private String jar_package;
    private String advanced_parameters;
    private String cron;
    private String run_info;
    private String oozie_hue_uuid;
    private String workflow_hue_uuid;
    private int state;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date start_time;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date end_time;
}
