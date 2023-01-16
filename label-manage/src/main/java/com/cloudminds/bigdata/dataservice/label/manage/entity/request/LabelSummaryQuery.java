package com.cloudminds.bigdata.dataservice.label.manage.entity.request;

import lombok.Data;

@Data
public class LabelSummaryQuery {
    private int tag_object_id;
    private int query_type;  //1 对象的标签数统计 2 对象的标签状态数统计 3:对象类目下的标签数统计 4：对象类目下的标签状态数统计
}
