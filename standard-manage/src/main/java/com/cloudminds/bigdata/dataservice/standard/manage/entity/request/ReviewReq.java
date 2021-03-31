package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;

import lombok.Data;

@Data
public class ReviewReq {
	private int id;
	private String message;
	private boolean pass=true;
}
