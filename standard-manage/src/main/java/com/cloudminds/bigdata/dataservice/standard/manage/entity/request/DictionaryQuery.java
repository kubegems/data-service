package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;

import lombok.Data;

@Data
public class DictionaryQuery {
    private String code;
    private String zh_name;
    private int page;
    private int size;
    private int classify_id;
}
