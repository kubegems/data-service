package com.cloudminds.bigdata.dataservice.quoto.manage.entity.request;

import lombok.Data;

import java.util.Map;

@Data
public class ExpressInfoReq {
	private String express;
	private Map<String,Object> parm_value;
}
