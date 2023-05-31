package com.cloudminds.bigdata.dataservice.quoto.manage.entity.response;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class QuotoNeedParmResponse {
    private int id;
    private String name;
    private String field;
    private String adjective;
    private String quotos;
    private List<Adjective> needParmAdjective=new ArrayList<>();
}
