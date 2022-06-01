package com.cloudminds.bigdata.dataservice.label.manage.entity.response;

import lombok.Data;

@Data
public class CommonResponse {
	private boolean success=true;
	private String message="请求成功";
	private Object data;
}
