package com.cloudminds.bigdata.dataservice.standard.manage.controller;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Dictionary;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.*;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.service.DictionaryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/standard/dictionary")
public class DictionaryControl {
    @Autowired
    private DictionaryService dictionaryService;

    // 创建字典
    @RequestMapping(value = "add", method = RequestMethod.POST)
    public CommonResponse insertDictionary(@RequestBody Dictionary dictionary) {
        return dictionaryService.insertDictionary(dictionary);
    }

    // 检查是否唯一
    @RequestMapping(value = "checkUnique", method = RequestMethod.POST)
    public CommonResponse checkUnique(@RequestBody CheckReq checkReq) {
        return dictionaryService.checkUnique(checkReq);
    }

    // 批量删除字典
    @RequestMapping(value = "batchDelete", method = RequestMethod.POST)
    public CommonResponse batchDeleteDictionary(@RequestBody BatchDeleteReq batchDeleteReq) {
        return dictionaryService.bachDeleteDictionary(batchDeleteReq);
    }

    // 编辑字典
    @RequestMapping(value = "update", method = RequestMethod.POST)
    public CommonResponse updateDictionary(@RequestBody Dictionary dictionary) {
        return dictionaryService.updateDictionary(dictionary);
    }

    // 发布字典
    @RequestMapping(value = "online", method = RequestMethod.POST)
    public CommonResponse onlineDictionary(@RequestBody DeleteReq onlineReq) {
        return dictionaryService.onlineDictionary(onlineReq);
    }

    // 下线字典
    @RequestMapping(value = "offline", method = RequestMethod.POST)
    public CommonResponse offlineDictionary(@RequestBody DeleteReq offlineReq) {
        return dictionaryService.offlineDictionary(offlineReq);
    }


    // 查询字典
    @RequestMapping(value = "query", method = RequestMethod.POST)
    public CommonQueryResponse queryDictionary(@RequestBody DictionaryQuery dictionaryQuery) {
        return dictionaryService.findDictionary(dictionaryQuery);
    }

    //上传文件获取字典信息
    @RequestMapping(value = "analysisFile", method = RequestMethod.POST)
    public CommonResponse analysisFile(@RequestParam MultipartFile file, HttpServletRequest request)  {
        return dictionaryService.analysisFile(file);
    }
}
