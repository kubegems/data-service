package com.cloudminds.bigdata.dataservice.quoto.search.controller;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset.*;
import com.cloudminds.bigdata.dataservice.quoto.search.service.DataSetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/search/dataset")
public class DataSetControl {
    @Autowired
    private DataSetService dataSetService;

    //增加目录
    @RequestMapping(value = "addDirectory", method = RequestMethod.POST)
    public CommonResponse addDirectory(@RequestBody Directory directory) {
        return dataSetService.addDirectory(directory);
    }

    //删除目录
    @RequestMapping(value = "deleteDirectory", method = RequestMethod.POST)
    public CommonResponse deleteDirectory(@RequestBody DeleteReq deleteReq) {
        return dataSetService.deleteDirectory(deleteReq);
    }

    //更新目录
    @RequestMapping(value = "updateDirectory", method = RequestMethod.POST)
    public CommonResponse updateDirectory(@RequestBody Directory directory) {
        return dataSetService.updateDirectory(directory);
    }

    //查询目录
    @RequestMapping(value = "queryDirectory", method = RequestMethod.GET)
    public CommonResponse queryDirectory(String creator, int pid) {
        return dataSetService.queryDirectory(creator, pid);
    }

    //新建数据集
    @RequestMapping(value = "add", method = RequestMethod.POST)
    public CommonResponse addDataset(DataSetAddReq dataSetAddReq) {
        return dataSetService.addDataset(dataSetAddReq);
    }

    //更新数据集
    @RequestMapping(value = "update", method = RequestMethod.POST)
    public CommonResponse updateDataset(DataSetAddReq dataSetAddReq) {
        return dataSetService.updateDataset(dataSetAddReq);
    }

    //删除数据集
    @RequestMapping(value = "delete", method = RequestMethod.POST)
    public CommonResponse deleteDataset(@RequestBody DeleteReq deleteReq) {
        return dataSetService.deleteDataset(deleteReq);
    }

    //分页查询数据集
    @RequestMapping(value = "query", method = RequestMethod.POST)
    public CommonResponse queryDateSet(@RequestBody DataSetQuery dataSetQuery) {
        return dataSetService.queryDateSet(dataSetQuery);
    }

    @RequestMapping(value = "queryById", method = RequestMethod.GET)
    public CommonResponse queryDateSetById(int id) {
        return dataSetService.queryDateSetById(id);
    }

    //全量查询数据集
    @RequestMapping(value = "queryAll", method = RequestMethod.GET)
    public CommonResponse queryAllDateSet(String creator, int directory_id) {
        return dataSetService.queryAllDateSet(creator, directory_id);
    }

    //查询数据集的总数和数据
    @RequestMapping(value = "queryData", method = RequestMethod.POST)
    public CommonResponse queryData(@RequestBody QueryDataReq queryDataReq) {
        return dataSetService.queryData(queryDataReq);
    }

    //sql预校验
    @RequestMapping(value = "checkSql", method = RequestMethod.POST)
    public CommonResponse checkSql(@RequestBody CheckSqlReq checkSqlReq) {
        return dataSetService.checkSql(checkSqlReq);
    }

    //查询数据占比
    @RequestMapping(value = "dataAccount", method = RequestMethod.POST)
    public CommonResponse dataAccount(@RequestBody DataAccountReq dataAccountReq) {
        return dataSetService.dataAccount(dataAccountReq);
    }

    // 获取指标调用文档
    @RequestMapping(value = "queryDataSetApiDoc", method = RequestMethod.GET)
    public CommonResponse queryDataSetApiDoc(int id) {
        CommonResponse CommonResponse = dataSetService.queryDataSetApiDoc(id);
        return CommonResponse;
    }

    //数据下载
    @RequestMapping(value = "/downloadData", method = RequestMethod.GET)
    public CommonResponse downloadData(int id) {
        return dataSetService.downloadData(id);
    }

    //csv追加数据
    @RequestMapping(value = "/csvUploadData", method = RequestMethod.POST)
    public CommonResponse csvUploadData(int id, MultipartFile file, boolean cover) {
        return dataSetService.csvUploadData(id, file, cover);
    }


    //增加任务
    @RequestMapping(value = "addTask", method = RequestMethod.POST)
    public CommonResponse addDatasetTask(@RequestBody DataSetTask dataSetTask) {
        return dataSetService.addDatasetTask(dataSetTask);
    }

    //更新任务
    @RequestMapping(value = "updateTask", method = RequestMethod.POST)
    public CommonResponse updateDatasetTask(@RequestBody DataSetTask dataSetTask) {
        return dataSetService.updateDatasetTask(dataSetTask);
    }

    //删除任务
    @RequestMapping(value = "deleteTask", method = RequestMethod.POST)
    public CommonResponse deleteDatasetTask(@RequestBody DeleteReq deleteReq) {
        return dataSetService.deleteDatasetTask(deleteReq);
    }

    //查询任务
    @RequestMapping(value = "queryTask", method = RequestMethod.GET)
    public CommonResponse queryDatasetTask(int data_set_id) {
        return dataSetService.queryDatasetTask(data_set_id);
    }

    //更新任务的状态
    @RequestMapping(value = "updateTaskState", method = RequestMethod.POST)
    public CommonResponse updateTaskState(@RequestBody UpdateTaskStateReq updateTaskStateReq){
        return dataSetService.updateTaskState(updateTaskStateReq);
    }
}
