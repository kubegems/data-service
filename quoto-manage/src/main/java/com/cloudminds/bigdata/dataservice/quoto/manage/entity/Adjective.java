package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.BaseEntity;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = false)
@Data
public class Adjective extends BaseEntity {
	private int id;
	private int dimension_id;
	private String column_name;
	private String name;
	private String code;
	private int type;
	private int quotoNum;
	private int req_parm_type;
	private String req_parm;
	private List<Field> fields;
}
