package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;

import lombok.Data;

@Data
public class EventBatchDeleteReq {
	private String[] event_codes;
}
