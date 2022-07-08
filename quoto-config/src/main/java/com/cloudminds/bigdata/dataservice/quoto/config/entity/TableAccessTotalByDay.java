package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class TableAccessTotalByDay {
    private String date;
    private int total;
}
