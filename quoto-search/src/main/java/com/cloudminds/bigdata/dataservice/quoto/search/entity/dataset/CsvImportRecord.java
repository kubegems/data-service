package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import lombok.Data;

@Data
public class CsvImportRecord {
    private String name;
    private String time;
    private String userName;
    private boolean cover;
}
