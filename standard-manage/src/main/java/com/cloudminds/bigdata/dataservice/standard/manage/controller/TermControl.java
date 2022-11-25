package com.cloudminds.bigdata.dataservice.standard.manage.controller;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.service.TermService;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/standard/term")
public class TermControl {
    @Autowired
    private TermService termService;

    // 创建术语
    @RequestMapping(value = "add", method = RequestMethod.POST)
    public CommonResponse insertTerm(@RequestBody Term term) {
        return termService.insertTerm(term);
    }

    @RequestMapping(value = "batchAdd", method = RequestMethod.POST)
    public CommonResponse batchAddTerm(@RequestBody BatchAddReq batchAddReq) {
        return termService.batchAddTerm(batchAddReq);
    }

    // 检查是否唯一
    @RequestMapping(value = "checkUnique", method = RequestMethod.POST)
    public CommonResponse checkUnique(@RequestBody CheckReq checkReq) {
        return termService.checkUnique(checkReq);
    }

    // 删除术语
    @RequestMapping(value = "delete", method = RequestMethod.POST)
    public CommonResponse deleteTerm(@RequestBody DeleteReq deleteReq) {
        return termService.deleteTerm(deleteReq);
    }

    // 批量删除术语
    @RequestMapping(value = "batchDelete", method = RequestMethod.POST)
    public CommonResponse batchDeleteTerm(@RequestBody BatchDeleteReq batchDeleteReq) {
        return termService.bachDeleteTerm(batchDeleteReq);
    }

    // 编辑术语
    @RequestMapping(value = "update", method = RequestMethod.POST)
    public CommonResponse updateTerm(@RequestBody Term term) {
        return termService.updateTerm(term);
    }

    // 查询术语
    @RequestMapping(value = "query", method = RequestMethod.POST)
    public CommonQueryResponse queryTerm(@RequestBody TermQuery termQuery) {
        return termService.findTerm(termQuery);
    }

    //上传文件获取字典信息
    @RequestMapping(value = "analysisFile", method = RequestMethod.POST)
    public CommonResponse analysisFile(@RequestParam MultipartFile file, HttpServletRequest request) {
        return termService.analysisFile(file);
    }
}
