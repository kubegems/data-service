package com.cloudminds.bigdata.dataservice.quoto.config.entity;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

@EqualsAndHashCode(callSuper = false)
@Data
public class UserToken extends BaseEntity{
    private int id;
    private String user_name;
    private String token;
    private int[] tables;
    private String[] table_names;
    private String creator;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date create_time;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date update_time;
}
