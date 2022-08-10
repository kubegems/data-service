package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import java.sql.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.Data;

@Data
public class QuotoAccessHistory {
	private int id;
	private int quoto_id;
	private String quoto_name;
	private String business;
	private String theme;
	private int level;
	private int type;
	private boolean success;
	private String message;
	@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date create_time;
}
