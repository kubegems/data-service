package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.QuotoTag;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Tag;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.TagMapper;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TagService {
    @Autowired
    private TagMapper tagMapper;

    public CommonResponse insertTag(Tag tag) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数
        if (tag == null || StringUtils.isEmpty(tag.getName()) || StringUtils.isEmpty(tag.getCreator()) || StringUtils.isEmpty(tag.getColor())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("tag名,创建者,颜色不能为空");
            return commonResponse;
        }
        //判断tag是否存在
        if (tagMapper.findTagByNameAndCreator(tag.getName(), tag.getCreator()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("此tag已经存在了,请不要重复添加");
            return commonResponse;
        }

        //插入数据
        if (tagMapper.insertTag(tag) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("添加tag失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse updateTag(Tag tag) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数
        if (tag == null || StringUtils.isEmpty(tag.getName()) || StringUtils.isEmpty(tag.getColor())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("tag名和颜色不能为空");
            return commonResponse;
        }

        Tag oldTag = tagMapper.findTayById(tag.getId());
        if (oldTag == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("tag不存在");
            return commonResponse;
        }

        if(!tag.getName().equals(oldTag.getName())){
            //判断tag是否存在
            if (tagMapper.findTagByNameAndCreator(tag.getName(), oldTag.getCreator()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("此tag已经存在了,请不要重复添加");
                return commonResponse;
            }
        }

        if(tagMapper.updateTag(tag)<1){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新tag失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse deleteTag(int id) {
        CommonResponse commonResponse = new CommonResponse();
        Tag oldTag = tagMapper.findTayById(id);
        if(oldTag==null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("tag不存在");
            return commonResponse;
        }
        //删除tag
        if(tagMapper.deleteTag(id)<1){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryTag(String creator) {
        CommonResponse commonResponse = new CommonResponse();
        if(StringUtils.isEmpty(creator)){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("creator不能为空");
            return commonResponse;
        }
        commonResponse.setData(tagMapper.queryTagByCreator(creator));
        return commonResponse;
    }

    public CommonResponse quotoAddTag(QuotoTag quotoTag) {
        CommonResponse commonResponse = new CommonResponse();
        if (quotoTag == null || StringUtils.isEmpty(quotoTag.getCreator())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("创建者不能为空");
            return commonResponse;
        }
        //判断quoto是否已经存在此tag
        if(tagMapper.queryQuotoTagByTagIdAndQuotoId(quotoTag.getTag_id(), quotoTag.getQuoto_id())!=null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("指标已经打过此tag,不能重复打");
            return commonResponse;
        }
        //增加记录
        try{
           if(tagMapper.insertQuotoTag(quotoTag)<1){
               commonResponse.setSuccess(false);
               commonResponse.setMessage("tag添加失败,请稍后再试");
               return commonResponse;
           }
        }catch (Exception e){
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse quotoRemoveTag(int tag_id,int quoto_id) {
        CommonResponse commonResponse = new CommonResponse();
        if(tagMapper.queryQuotoTagByTagIdAndQuotoId(tag_id,quoto_id)==null){
            return commonResponse;
        }
        if(tagMapper.deleteQuotoTag(tag_id,quoto_id)<1){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除失败,请稍后再试");
            return commonResponse;
        }
        return commonResponse;
    }
}
