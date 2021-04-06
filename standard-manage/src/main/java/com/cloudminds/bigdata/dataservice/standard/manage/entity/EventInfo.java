package com.cloudminds.bigdata.dataservice.standard.manage.entity;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class EventInfo extends BaseEntity {
	private int id;
	private String event_name;
	private String event_code;
	private String model_name;
	private String model_version;
	private String version;
	private int type;
	private String type_version;
	private String jira_num;
	private String message;
	private int state;
	private boolean uniqueVersion=true;
	private boolean changeFields=true;
	private List<Field> fields;
}
