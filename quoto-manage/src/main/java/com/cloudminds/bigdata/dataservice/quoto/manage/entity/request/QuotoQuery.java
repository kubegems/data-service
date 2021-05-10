package com.cloudminds.bigdata.dataservice.quoto.manage.entity.request;

import lombok.Data;

@Data
public class QuotoQuery {
	private int type = -1;
	private int businessId=-1;
	private String name;
	private String field;
	private int page;
	private int size;
}
