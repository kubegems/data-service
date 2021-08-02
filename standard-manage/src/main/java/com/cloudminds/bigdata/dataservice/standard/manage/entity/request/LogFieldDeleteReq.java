package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;
import lombok.Data;

@Data
public class LogFieldDeleteReq {
    private String version;
    private int type;
    private String model_name;
}
