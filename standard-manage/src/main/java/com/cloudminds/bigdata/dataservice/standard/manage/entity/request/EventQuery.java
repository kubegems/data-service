package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;

import lombok.Data;

@Data
public class EventQuery {
	private int type = -1;
	private String model_name;
	private String event_name;
	private String event_code;
	private int state = -1;
	private int page;
	private int size;
}
