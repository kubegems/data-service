package com.cloudminds.bigdata.dataservice.quoto.config.controller;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.config.service.MetaDataService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/dataservice/metadata")
public class MetaDataControl {
    @Autowired
    private MetaDataService metaDataService;

    //创建table
    @RequestMapping(value = "addTable", method = RequestMethod.POST)
    public CommonResponse addTable(@RequestBody MetaDataTable metaDataTable) {
        return metaDataService.addTable(metaDataTable);
    }

    @RequestMapping(value = "precomputationDdl", method = RequestMethod.POST)
    public CommonResponse precomputationDdl(@RequestBody MetaDataTable metaDataTable) {
        return metaDataService.precomputationDdl(metaDataTable);
    }

    //更新table
    @RequestMapping(value = "updateTable", method = RequestMethod.POST)
    public CommonResponse updateTable(@RequestBody UpdateMetaDataTableReq updateMetaDataTableReq) {
        return metaDataService.updateTable(updateMetaDataTableReq);
    }

    //删除table
    @RequestMapping(value = "deleteTable", method = RequestMethod.POST)
    public CommonResponse deleteTable(@RequestBody DeleteReq deleteReq) {
        return metaDataService.deleteTable(deleteReq);
    }

    //根据库查询table
    @RequestMapping(value = "findTable", method = RequestMethod.POST)
    public CommonResponse findTable(@RequestBody QueryMetaDataTableReq queryMetaDataTableReq) {
        return metaDataService.findTable(queryMetaDataTableReq);
    }

    //根据库表查询table
    @RequestMapping(value = "findTableByTableName", method = RequestMethod.GET)
    public CommonResponse findTableByTableName(int table_type, String database_name, String table_name) {
        return metaDataService.findTableByTableName(table_type, database_name, table_name);
    }


    //上传文件获取字典信息
    @RequestMapping(value = "analysisFile", method = RequestMethod.POST)
    public CommonResponse analysisFile(@RequestParam MultipartFile file, HttpServletRequest request) {
        return metaDataService.analysisFile(file);
    }

    @RequestMapping(value = "historyDataAddDataBase", method = RequestMethod.POST)
    public CommonResponse historyDataAddDataBase(@RequestBody HistoryDataAddDataBase historyDataAddDataBase) {
        return metaDataService.historyDataAddDataBase(historyDataAddDataBase);
    }
}
