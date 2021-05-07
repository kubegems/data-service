package com.cloudminds.bigdata.dataservice.quoto.manage.entity.request;

import lombok.Data;

@Data
public class AdjectiveQuery {
	private int type = -1;
	private String name;
	private int page;
	private int size;
}
