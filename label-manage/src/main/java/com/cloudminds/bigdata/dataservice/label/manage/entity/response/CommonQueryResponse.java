package com.cloudminds.bigdata.dataservice.label.manage.entity.response;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class CommonQueryResponse extends CommonResponse {
	private int currentPage;
	private int total;
}
