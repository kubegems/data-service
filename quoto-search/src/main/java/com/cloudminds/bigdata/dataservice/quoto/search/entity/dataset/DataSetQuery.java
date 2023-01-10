package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import lombok.Data;

@Data
public class DataSetQuery {
	private int data_type;
	private String creator;
	private String name;
	private int page;
	private int size;
}
