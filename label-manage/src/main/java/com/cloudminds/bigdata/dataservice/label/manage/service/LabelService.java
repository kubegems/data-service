package com.cloudminds.bigdata.dataservice.label.manage.service;

import com.cloudminds.bigdata.dataservice.label.manage.entity.TagItem;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.label.manage.entity.response.TagInfo;
import com.cloudminds.bigdata.dataservice.label.manage.mapper.LabelItemMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class LabelService {
    @Autowired
    private LabelItemMapper labelItemMapper;

    // 查询事件详情
    public CommonResponse findLabelItemByPid(String pid,int value) {
        CommonResponse commonResponse = new CommonResponse();
        if(pid == null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("pid不能为空");
            return commonResponse;
        }
        //pid 为0 首次查询分类
        if(pid.equals("0")) {
            commonResponse.setData(labelItemMapper.findTagInfoFromCate(pid));
            return commonResponse;
        }
        //pid不为0 value不为0
        if(value>0){
            TagItem labelItem = labelItemMapper.findTagItemByTagId(pid);
            if(labelItem==null){
                commonResponse.setSuccess(false);
                commonResponse.setMessage("标签不存在");
                return commonResponse;
            }
            //
            if(labelItem.getValue_type()!=1){
                commonResponse.setSuccess(false);
                commonResponse.setMessage("已经是最后层级了");
                return commonResponse;
            }
            //查询枚举类型
            commonResponse.setData(labelItemMapper.findTagInfoFromTagEnumValue(pid));
            return commonResponse;
        }

        //pid不为0 value为0
        List<TagInfo> taginfos = labelItemMapper.findTagInfoFromCate(pid);
        if(taginfos==null||taginfos.isEmpty()){
            commonResponse.setData(labelItemMapper.findTagInfoFromTagItem(pid));
        }else{
            commonResponse.setData(taginfos);
        }
        return commonResponse;
    }
}
