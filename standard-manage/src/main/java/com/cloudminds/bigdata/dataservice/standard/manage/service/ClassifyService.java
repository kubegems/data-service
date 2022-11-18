package com.cloudminds.bigdata.dataservice.standard.manage.service;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Classify;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.Dictionary;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.ClassifyMapper;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.DictionaryMapper;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.TermMapper;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ClassifyService {
    @Autowired
    private ClassifyMapper classifyMapper;
    @Autowired
    private TermMapper termMapper;
    @Autowired
    private DictionaryMapper dictionaryMapper;

    //增加分类
    public CommonResponse insertClassify(Classify classify) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数
        if(classify==null || StringUtils.isEmpty(classify.getName())){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名称不能为空");
            return commonResponse;
        }

        //查询是否已存在
        if(classifyMapper.findClassifyByName(classify.getPid(),classify.getType(),classify.getName())!=null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("分类名称已存在,请不要重复添加");
            return commonResponse;
        }

        //查询pid是否存在
        if(classify.getPid()>0){
            Classify pidClassify = classifyMapper.findClassifyById(classify.getPid());
            if(pidClassify==null){
                commonResponse.setSuccess(false);
                commonResponse.setMessage("上级分类不存在");
                return commonResponse;
            }
            if(pidClassify.getPid()>0){
                Classify pidPidClassify = classifyMapper.findClassifyById(pidClassify.getPid());
                if(pidPidClassify==null){
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("上上级分类不存在");
                    return commonResponse;
                }
                if(pidPidClassify.getPid()>0&&(classify.getType()==1 || classify.getType()==2)){
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("只支持三级分类哦");
                    return commonResponse;
                }
            }
        }

        //入分类
        if(classifyMapper.insertClassify(classify)<=0){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("增加分类失败,请稍后再试或者联系管理员");
            return commonResponse;
        }
        commonResponse.setData(classify.getId());
        return commonResponse;
    }

    //更新分类
    public CommonResponse updateClassify(Classify classify) {
        CommonResponse commonResponse = new CommonResponse();
        //参数校验
        if(classify==null||StringUtils.isEmpty(classify.getName())){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("名称不能为空");
            return commonResponse;
        }
        //校验分类是否存在
        Classify oldClassify = classifyMapper.findClassifyById(classify.getId());
        if(oldClassify==null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原始分类不存在");
            return commonResponse;
        }
        if(!classify.getName().equals(oldClassify.getName())){
            //判断分类是否存在
            if(classifyMapper.findClassifyByName(oldClassify.getPid(), oldClassify.getType(), classify.getName())!=null){
                commonResponse.setSuccess(false);
                commonResponse.setMessage("分类已存在,请不要重复添加");
                return commonResponse;
            }
            oldClassify.setName(classify.getName());
        }
        //更新分类
        if(classifyMapper.updateClassifyName(classify.getId(), classify.getName())<=0){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新分类失败,请稍后再试或者联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    //删除分类
    public CommonResponse deleteClassify(int id) {
        CommonResponse commonResponse = new CommonResponse();
        Classify classify = classifyMapper.findClassifyById(id);
        if(classify==null){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("分类不存在");
            return commonResponse;
        }
        //判断分类下面有没有子分类
        List<Classify> subClassify = classifyMapper.findClassifyByPid(id,classify.getType());
        if(subClassify!=null&&subClassify.size()>0){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("有子分类存在,不能删除");
            return commonResponse;
        }
        //判断分类是否有使用
        if(classify.getType()==1){
           List<Term> terms = termMapper.findTermByClassify(id);
            if(terms!=null&&terms.size()>0){
                commonResponse.setSuccess(false);
                commonResponse.setMessage("该分类下有术语,不能删除");
                return commonResponse;
            }
        }else{
            List<Dictionary> dictionaries = dictionaryMapper.queryDictionaryByClassify(id);
            if(dictionaries!=null&&dictionaries.size()>0){
                commonResponse.setSuccess(false);
                commonResponse.setMessage("该分类下有字典,不能删除");
                return commonResponse;
            }
        }

        //删除分类
        if(classifyMapper.deleteClassifyName(id)<=0){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除分类失败,请稍后再试或者联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    //查询分类
    public CommonResponse queryClassify(int pid, int type) {
        CommonResponse commonResponse = new CommonResponse();
        if(pid==-1){
            commonResponse.setData(classifyMapper.findAllClassify(type));
        }else {
            commonResponse.setData(classifyMapper.findClassifyByPid(pid, type));
        }
        return commonResponse;
    }
}
