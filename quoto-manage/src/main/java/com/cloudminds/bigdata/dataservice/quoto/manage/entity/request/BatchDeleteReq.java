package com.cloudminds.bigdata.dataservice.quoto.manage.entity.request;

import lombok.Data;

@Data
public class BatchDeleteReq {
	private int[] ids;
}
