package com.cloudminds.bigdata.dataservice.quoto.search.entity;


import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.Date;

@Data
public class BaseEntity {
	private String creator;
    private String descr;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date create_time;
	@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date update_time;
	private boolean deleted;
}
