package com.cloudminds.bigdata.dataservice.quoto.search.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class CommonScrollResponse extends CommonResponse{
	private Object scroll_id;
}
