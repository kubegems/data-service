package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import lombok.Data;
import org.springframework.web.multipart.MultipartFile;

@Data
public class DataSetAddReq extends DataSet {
    MultipartFile file;
    private String data_column_string;
    private boolean cover;
    private boolean table_reCreate = false;
}
