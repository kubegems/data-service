package com.cloudminds.bigdata.dataservice.label.manage.entity.response;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItemTask;
import lombok.Data;

@Data
public class TagItemTaskExtendinfo extends TagItemTask {
    private String tag_name;
    private String tag_state;
}
