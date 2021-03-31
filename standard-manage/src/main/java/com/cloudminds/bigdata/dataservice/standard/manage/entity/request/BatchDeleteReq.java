package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;

import lombok.Data;

@Data
public class BatchDeleteReq {
	private int[] ids;
}
