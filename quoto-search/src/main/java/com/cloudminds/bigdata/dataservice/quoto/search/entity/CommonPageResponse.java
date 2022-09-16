package com.cloudminds.bigdata.dataservice.quoto.search.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class CommonPageResponse extends CommonResponse{
	private int currentPage;
	private long total;
}
