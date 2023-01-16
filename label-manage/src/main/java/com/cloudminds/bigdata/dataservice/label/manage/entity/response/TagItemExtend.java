package com.cloudminds.bigdata.dataservice.label.manage.entity.response;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItem;
import lombok.Data;

@Data
public class TagItemExtend extends TagItem {
    private int task_state;
    private String cron;
    private Integer task_id;
}
