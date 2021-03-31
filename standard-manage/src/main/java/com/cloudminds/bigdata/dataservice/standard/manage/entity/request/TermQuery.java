package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;

import lombok.Data;

@Data
public class TermQuery {
	private String term_field;
	private String zh_name;
	private int page;
	private int size;
}
