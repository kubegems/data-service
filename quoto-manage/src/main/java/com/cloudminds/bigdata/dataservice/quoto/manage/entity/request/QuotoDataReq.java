package com.cloudminds.bigdata.dataservice.quoto.manage.entity.request;

import java.util.Set;

import lombok.Data;

@Data
public class QuotoDataReq {
private Integer id;
private String name;
private String field;
private Integer page;
private Integer count;
private Set<String> order;
private Boolean acs;
}
