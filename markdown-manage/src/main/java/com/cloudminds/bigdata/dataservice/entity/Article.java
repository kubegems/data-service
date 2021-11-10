package com.cloudminds.bigdata.dataservice.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class Article extends BaseEntity{
    private int id;
    private String title;
    private String content;
}
