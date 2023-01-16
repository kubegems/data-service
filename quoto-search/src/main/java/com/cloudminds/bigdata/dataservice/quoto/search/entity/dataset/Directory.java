package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.BaseEntity;
import lombok.Data;

@Data
public class Directory extends BaseEntity {
    private int id;
    private String name;
    private int pid;
}
