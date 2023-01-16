package com.cloudminds.bigdata.dataservice.label.manage.entity.request;

import lombok.Data;

@Data
public class LabelItemComplexQuery {
	private int tag_object_id;
	private int page;
	private int size;
	private int state=-1;
}
