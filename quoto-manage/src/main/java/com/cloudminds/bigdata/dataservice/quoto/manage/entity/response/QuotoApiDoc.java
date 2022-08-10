package com.cloudminds.bigdata.dataservice.quoto.manage.entity.response;

import lombok.Data;

import java.util.List;

@Data
public class QuotoApiDoc {
    private List<ExtendField> requestParament;
    private String service_path="/quotoManage/quoto/queryQuotoData";
    private String method="post";
}
