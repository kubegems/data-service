package com.cloudminds.bigdata.dataservice.standard.manage.service;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.EventQuery;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.LogFieldDeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacos.api.utils.StringUtils;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.CommonField;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.ModelField;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.CommonFieldMapper;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.ModelFieldMapper;

import java.text.SimpleDateFormat;
import java.util.Date;

@Service
public class LogFieldService {
    @Autowired
    private CommonFieldMapper commonFieldMapper;

    @Autowired
    private ModelFieldMapper modelFieldMapper;

    public CommonResponse getCommonLogField(int type, String version) {
        CommonResponse commonResponse = new CommonResponse();
        CommonField commonField = null;
        if (StringUtils.isEmpty(version)) {
            //获取最新的字段
            commonField = commonFieldMapper.findLastCommonField(type);
        } else {
            //获取指定版本的字段
            commonField = commonFieldMapper.findCommonFieldByVersion(type, version);
        }
        if (commonField == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("没有通用的字段,请联系管理员添加");
        } else {
            commonResponse.setData(commonField);
        }
        return commonResponse;
    }

    public CommonResponse getModelLogField(int type, String version, String model_name) {
        CommonResponse commonResponse = new CommonResponse();
        ModelField modelField = null;
        if (StringUtils.isEmpty(model_name)) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("模块名不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(version)) {
            //获取最新的字段
            modelField = modelFieldMapper.findLastModelField(type, model_name);
        } else {
            //获取指定版本的字段
            modelField = modelFieldMapper.findModelFieldByVersion(type, version, model_name);
        }
        if (modelField == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("没有通用的字段,请联系管理员添加");
        } else {
            commonResponse.setData(modelField);
        }
        return commonResponse;
    }

    public CommonResponse getModelInfo(int type) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(modelFieldMapper.findModel(type));
        return commonResponse;
    }

    public CommonResponse deleteCommonLogField(LogFieldDeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(deleteReq.getVersion())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("版本号不能为空");
            return commonResponse;
        }
        if (commonFieldMapper.findCommonFieldNum(deleteReq.getType()) <= 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("只有一个版本的时候，不能删除");
            return commonResponse;
        }
        if (commonFieldMapper.deleteCommonField(deleteReq.getType(), deleteReq.getVersion()) <= 0) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse deleteModelLogField(LogFieldDeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(deleteReq.getVersion())||StringUtils.isEmpty(deleteReq.getModel_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("版本号或者版本名称不能为空");
            return commonResponse;
        }
        if (modelFieldMapper.deleteModelField(deleteReq.getType(), deleteReq.getVersion(),deleteReq.getModel_name()) <= 0) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse updateCommonLogField(CommonField commonField) {
        CommonResponse commonResponse = new CommonResponse();
        //校验版本号不能为空
        if (StringUtils.isEmpty(commonField.getVersion())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("版本号不能为空");
            return commonResponse;
        }
        //此版本的通用日志存在
        CommonField oldCommonField = commonFieldMapper.findCommonFieldByVersion(commonField.getType(), commonField.getVersion());
        if (oldCommonField == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原始版本不存在,请刷新后再试");
            return commonResponse;
        }
        //如果只是改变了描述可以直接保存
        if (!commonField.isChangeFields()) {
            oldCommonField.setDescr(commonField.getDescr());
            if (commonFieldMapper.updateCommonField(oldCommonField)!= 1) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("更新失败,请稍后再试");
                return commonResponse;
            }
            commonResponse.setMessage("描述更新成功");
            return commonResponse;
        }
        //如果版本号是当天的，可以直接保存，不增加新版本
        String version = new SimpleDateFormat("yyyy.MM.dd").format(new Date());
        if(version.equals(commonField.getVersion())){
            oldCommonField.setDescr(commonField.getDescr());
            oldCommonField.setFields(commonField.getFields());
            if (commonFieldMapper.updateCommonField(oldCommonField)!= 1) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("更新失败,请稍后再试");
                return commonResponse;
            }
            commonResponse.setMessage("描述更新成功");
            return commonResponse;
        }

        //如果版本号不是当天的，历史版本不变，新增版本
        // 插入数据库
        commonField.setVersion(version);
        try {
            commonFieldMapper.insertCommonField(commonField);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新失败");
            return commonResponse;
        }

        return commonResponse;
    }

    public CommonResponse updateModelLogField(ModelField modelField) {
        CommonResponse commonResponse = new CommonResponse();
        //校验版本号不能为空
        if (StringUtils.isEmpty(modelField.getVersion())||StringUtils.isEmpty(modelField.getModel_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("版本号和模块名称不能为空");
            return commonResponse;
        }
        //此版本的通用日志存在
        ModelField oldModelField = modelFieldMapper.findModelFieldByVersion(modelField.getType(), modelField.getVersion(),modelField.getModel_name());
        if (oldModelField == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原始版本不存在,请刷新后再试");
            return commonResponse;
        }
        //如果只是改变了描述可以直接保存
        if (!modelField.isChangeFields()) {
            oldModelField.setDescr(modelField.getDescr());
            if (modelFieldMapper.updateModelField(oldModelField)!= 1) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("更新失败,请稍后再试");
                return commonResponse;
            }
            commonResponse.setMessage("描述更新成功");
            return commonResponse;
        }
        //如果版本号是当天的，可以直接保存，不增加新版本
        String version = new SimpleDateFormat("yyyy.MM.dd").format(new Date());
        if(version.equals(modelField.getVersion())){
            oldModelField.setDescr(modelField.getDescr());
            oldModelField.setFields(modelField.getFields());
            if (modelFieldMapper.updateModelField(oldModelField)!= 1) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("更新失败,请稍后再试");
                return commonResponse;
            }
            commonResponse.setMessage("描述更新成功");
            return commonResponse;
        }

        //如果版本号不是当天的，历史版本不变，新增版本
        modelField.setVersion(version);
        modelField.setModel_id(oldModelField.getModel_id());
        // 插入数据库
        try {
            modelFieldMapper.insertModelField(modelField);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新失败");
            return commonResponse;
        }

        return commonResponse;
    }

    public CommonResponse insertModelLogField(ModelField modelField) {
        CommonResponse commonResponse = new CommonResponse();
        //校验版本号不能为空
        if (StringUtils.isEmpty(modelField.getModel_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("模块名称不能为空");
            return commonResponse;
        }
        //判断模块名是否重复
        if(modelFieldMapper.countModelFieldByModelName(modelField.getType(),modelField.getModel_name())>0){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("模块名已存在,请重新命名");
            return commonResponse;
        }

        //生成modelId
        String modelId = modelFieldMapper.findMaxModelId();
        if (StringUtils.isEmpty(modelId)) {
            modelId = "01";
        } else {
            modelId = (Integer.parseInt(modelId) + 1) + "";
            while (modelId.length() < 2) {
                modelId = "0" + modelId;
            }
        }
        modelField.setModel_id(modelId);

        //生成版本version
        String version = new SimpleDateFormat("yyyy.MM.dd").format(new Date());
        modelField.setVersion(version);
        //入库
        try {
            modelFieldMapper.insertModelField(modelField);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新失败");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonQueryResponse queryCommonLogField(EventQuery eventQuery) {
        CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
        String condition = "";
        if (eventQuery.getType() != -1) {
            condition = condition + "and type=" + eventQuery.getType()+" ";
        }
        condition =condition+ "order by create_time desc";
        int page = eventQuery.getPage();
        int size = eventQuery.getSize();
        int startLine = (page - 1) * size;
        commonQueryResponse.setData(commonFieldMapper.queryCommonField(condition, startLine, size));
        commonQueryResponse.setCurrentPage(eventQuery.getPage());
        commonQueryResponse.setTotal(commonFieldMapper.queryCommonFieldCount(condition));
        return commonQueryResponse;
    }
    public CommonQueryResponse queryModelLogField(EventQuery eventQuery) {
        CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
        String condition = "";
        if (eventQuery.getType() != -1) {
            condition = condition + "and type=" + eventQuery.getType()+" ";
        }

        if(eventQuery.getModel_name()!=null){
            condition=condition+"and model_name='"+eventQuery.getModel_name()+"' ";
        }
        condition =condition+ "order by create_time desc";
        int page = eventQuery.getPage();
        int size = eventQuery.getSize();
        int startLine = (page - 1) * size;
        commonQueryResponse.setData(modelFieldMapper.queryModelField(condition, startLine, size));
        commonQueryResponse.setCurrentPage(eventQuery.getPage());
        commonQueryResponse.setTotal(modelFieldMapper.queryModelFieldCount(condition));
        return commonQueryResponse;
    }

    public CommonResponse checkUnique(CheckReq checkReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (checkReq.getCheckValue() == null || checkReq.getCheckValue().equals("")) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("check的值不能为空");
            return commonResponse;
        }
        if (modelFieldMapper.countModelFieldByModelName(checkReq.getCheckflag(),checkReq.getCheckValue())>0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名字已存在,请重新命名");
        }
        return commonResponse;
    }
}
