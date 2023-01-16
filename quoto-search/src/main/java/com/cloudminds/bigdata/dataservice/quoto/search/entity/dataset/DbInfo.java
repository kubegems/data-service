package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class DbInfo extends BaseEntity {
	private int id;
	private String db_url;
	private String db_name;
	private String userName;
	private String password;
	private String service_name;
	private String service_path;
}
