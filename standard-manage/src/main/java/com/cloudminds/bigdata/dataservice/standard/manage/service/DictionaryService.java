package com.cloudminds.bigdata.dataservice.standard.manage.service;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Classify;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.Dictionary;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.DictionaryValue;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.DictionaryQuery;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.ClassifyMapper;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.DictionaryMapper;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.TermMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Service
public class DictionaryService {
    @Autowired
    private DictionaryMapper dictionaryMapper;
    @Autowired
    private ClassifyMapper classifyMapper;

    public CommonResponse insertDictionary(Dictionary dictionary) {
        CommonResponse commonResponse = new CommonResponse();
        //check入参
        if (dictionary == null || StringUtils.isEmpty(dictionary.getZh_name()) || StringUtils.isEmpty(dictionary.getCode()) || StringUtils.isEmpty(dictionary.getEn_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("字典名称 英文名称 标识不能为空");
            return commonResponse;
        }
        if (dictionaryMapper.findDictionaryByZh(dictionary.getZh_name()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("字典名称已存在");
            return commonResponse;
        }

        if (dictionaryMapper.findDictionaryByCode(dictionary.getCode()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标识已存在");
            return commonResponse;
        }

        if (dictionaryMapper.findDictionaryByEn(dictionary.getEn_name()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("英文名称已存在");
            return commonResponse;
        }

        //校验分类
       Classify classify = classifyMapper.findClassifyById(dictionary.getClassify_id());
        if(classify==null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("分类不存在");
            return commonResponse;
        }

        try {
            if (dictionaryMapper.insertDictionary(dictionary) <= 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("添加字典失败，请稍后再试");
            }
        } catch (Exception e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("添加字典失败，请联系管理员");
        }
        return commonResponse;
    }

    public CommonResponse checkUnique(CheckReq checkReq) {
        CommonResponse commonResponse = new CommonResponse();
        byte flag = checkReq.getCheckflag();
        Dictionary dictionary = null;
        if (checkReq.getCheckValue() == null || checkReq.getCheckValue().equals("")) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("check的值不能为空");
            return commonResponse;
        }
        if (flag == 0) {
            dictionary = dictionaryMapper.findDictionaryByZh(checkReq.getCheckValue());
        } else if (flag == 1) {
            dictionary = dictionaryMapper.findDictionaryByEn(checkReq.getCheckValue());
        } else if (flag == 2) {
            dictionary = dictionaryMapper.findDictionaryByCode(checkReq.getCheckValue());
        } else {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("不支持check的类型");
            return commonResponse;
        }
        if (dictionary != null) {
            commonResponse.setSuccess(false);
            commonResponse.setData(dictionary);
            commonResponse.setMessage("已存在,请重新命名");
        }
        return commonResponse;
    }

    public CommonResponse bachDeleteDictionary(BatchDeleteReq batchDeleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (batchDeleteReq.getIds() == null || batchDeleteReq.getIds().length == 0) {
            commonResponse.setMessage("请先选择要删除的行");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (dictionaryMapper.batchDeleteDictionary(batchDeleteReq.getIds()) <= 0) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse updateDictionary(Dictionary dictionary) {
        CommonResponse commonResponse = new CommonResponse();
        Dictionary oldDictionary = dictionaryMapper.findDictionaryById(dictionary.getId());
        if (oldDictionary == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("字典不存在");
            return commonResponse;
        }
        //校验参数
        if (!oldDictionary.getEn_name().equals(dictionary.getEn_name())) {
            if (dictionaryMapper.findDictionaryByEn(dictionary.getEn_name()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("英文名称已存在");
                return commonResponse;
            }
        }
        if (!oldDictionary.getZh_name().equals(dictionary.getZh_name())) {
            if (dictionaryMapper.findDictionaryByZh(dictionary.getZh_name()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("字典名称已存在");
                return commonResponse;
            }
        }

        if (!oldDictionary.getCode().equals(dictionary.getCode())) {
            if (dictionaryMapper.findDictionaryByCode(dictionary.getCode()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标识已存在");
                return commonResponse;
            }
        }
        if(dictionary.getClassify_id()!=oldDictionary.getClassify_id()){
            //校验分类
            Classify classify = classifyMapper.findClassifyById(dictionary.getClassify_id());
            if(classify==null){
                commonResponse.setSuccess(false);
                commonResponse.setMessage("分类不存在");
                return commonResponse;
            }
        }
        try {
            if (dictionaryMapper.updateDictionary(dictionary) <= 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("更新失败,请稍后再试");
                return commonResponse;
            }
        }catch (Exception e){
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新字典失败，请联系管理员");
        }
        return commonResponse;
    }

    public CommonResponse onlineDictionary(DeleteReq onlineReq) {
        CommonResponse commonResponse = new CommonResponse();
        Dictionary dictionary = dictionaryMapper.findDictionaryById(onlineReq.getId());
        if (dictionary == null) {
            commonResponse.setMessage("字典不存在");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        //判断字典是否已经上线了
        if (dictionary.getState() == 1) {
            return commonResponse;
        }
        if (dictionaryMapper.onlineDictionary(onlineReq.getId()) <= 0) {
            commonResponse.setMessage("上线失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse offlineDictionary(DeleteReq offlineReq) {
        CommonResponse commonResponse = new CommonResponse();
        Dictionary dictionary = dictionaryMapper.findDictionaryById(offlineReq.getId());
        if (dictionary == null) {
            commonResponse.setMessage("字典不存在");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (dictionary.getState() == 0) {
            commonResponse.setMessage("开发中的字典不能下线");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        //判断字典是否已经下线了
        if (dictionary.getState() == 2) {
            return commonResponse;
        }
        if (dictionaryMapper.offlineDictionary(offlineReq.getId()) <= 0) {
            commonResponse.setMessage("上线失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonQueryResponse findDictionary(DictionaryQuery dictionaryQuery) {
        CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
        String condition = "";
        if (!StringUtils.isEmpty(dictionaryQuery.getCode())) {
            condition="and d.code like '"+dictionaryQuery.getCode()+"%'";
        }
        if(!StringUtils.isEmpty(dictionaryQuery.getZh_name())){
            if(!condition.equals("")) {
                condition=condition+" ";
            }
            condition=condition+"and d.zh_name like '"+dictionaryQuery.getZh_name()+"%'";
        }
        if(dictionaryQuery.getClassify_id()>0){
            condition=condition+" and (one.id="+dictionaryQuery.getClassify_id()+" or two.id="+dictionaryQuery.getClassify_id()+" or three.id="+dictionaryQuery.getClassify_id()+")";
        }
        int page = dictionaryQuery.getPage();
        int size = dictionaryQuery.getSize();
        int startLine = (page - 1) * size;
        commonQueryResponse.setData(dictionaryMapper.queryDictionary(condition, startLine, size));
        commonQueryResponse.setCurrentPage(dictionaryQuery.getPage());
        commonQueryResponse.setTotal(dictionaryMapper.queryDictionaryCount(condition));
        return commonQueryResponse;
    }


    public CommonResponse analysisFile(MultipartFile file) {
        CommonResponse commonResponse = new CommonResponse();
        List<DictionaryValue> dictionaryValueList = new ArrayList<>();
        try {
            CSVReader csvReader = new CSVReaderBuilder(
                    new BufferedReader(
                            new InputStreamReader(file.getInputStream(), "utf-8"))).build();
            Iterator<String[]> iterator = csvReader.iterator();
            while (iterator.hasNext()) {
                String[] next = iterator.next();
                DictionaryValue dictionaryValue = new DictionaryValue();
                if(next.length>=1){
                    dictionaryValue.setCode(next[0]);
                }
                if(next.length>=2){
                    dictionaryValue.setDesc(next[1]);
                }
                dictionaryValueList.add(dictionaryValue);
            }
            commonResponse.setData(dictionaryValueList);
            return commonResponse;
        } catch (Exception e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
    }
}
