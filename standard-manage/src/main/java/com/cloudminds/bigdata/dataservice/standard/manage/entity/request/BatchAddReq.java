package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import lombok.Data;

import java.util.List;

@Data
public class BatchAddReq {
    private List<Term> terms;
}
