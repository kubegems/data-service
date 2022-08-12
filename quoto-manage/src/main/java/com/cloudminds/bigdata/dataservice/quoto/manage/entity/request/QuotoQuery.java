package com.cloudminds.bigdata.dataservice.quoto.manage.entity.request;

import lombok.Data;

@Data
public class QuotoQuery {
	private int type = -1;    //0原子指标 1衍生指标 2复合指标 3衍生和复合指标
	private int businessId=-1;
	private int business_process_id=-1;
	private int theme_id=-1;
	private int quoto_level=-1;
	private int state=-1;
	private String name;
	private String field;
	private int page;
	private int size;
}
