package com.cloudminds.bigdata.dataservice.label.manage.service;

import com.cloudminds.bigdata.dataservice.label.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.CodeInfoMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LabelService {
    @Autowired
    private CodeInfoMapper codeInfoMapper;

    // 查询事件详情
    public CommonResponse findCodeInfoByLikeCode(String code) {
        CommonResponse commonResponse = new CommonResponse();
        if(code == null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("code不能为空");
        }
        code = code + '%';
        commonResponse.setData(codeInfoMapper.findCodeInfoByLikeCode(code));
        return commonResponse;
    }
}
