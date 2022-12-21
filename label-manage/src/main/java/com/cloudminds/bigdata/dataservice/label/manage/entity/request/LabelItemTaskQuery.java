package com.cloudminds.bigdata.dataservice.label.manage.entity.request;

import lombok.Data;

@Data
public class LabelItemTaskQuery {
	private int tag_object_id;
	private String queryValue;
	private int page;
	private int size;
}
