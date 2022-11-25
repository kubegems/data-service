package com.cloudminds.bigdata.dataservice.standard.manage.service;

import com.cloudminds.bigdata.dataservice.standard.manage.controller.TermControl;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.Classify;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.DictionaryValue;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.*;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.ClassifyMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.TermMapper;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

@Service
public class TermService {
	@Autowired
	private TermMapper termMapper;
	@Autowired
	private ClassifyMapper classifyMapper;
	// 添加术语
	public CommonResponse insertTerm(Term term) {
		CommonResponse commonResponse = new CommonResponse();
		if(term==null || StringUtils.isEmpty(term.getZh_name()) || StringUtils.isEmpty(term.getTerm_field())|| StringUtils.isEmpty(term.getEn_name())){
			commonResponse.setSuccess(false);
			commonResponse.setMessage("中文名 英文名 编码不能为空");
			return commonResponse;
		}
		//校验分类
		Classify classify = classifyMapper.findClassifyById(term.getClassify_id());
		if(classify==null){
			commonResponse.setSuccess(false);
			commonResponse.setMessage("分类不存在");
			return commonResponse;
		}

		try {
			if (termMapper.insertTerm(term) <= 0) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("注册术语失败，请稍后再试");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			commonResponse.setSuccess(false);
			commonResponse.setMessage("注册术语失败，请稍后再试");
		}
		return commonResponse;
	}

	// check是否重复
	public CommonResponse checkUnique(CheckReq checkReq) {
		CommonResponse commonResponse = new CommonResponse();
		byte flag = checkReq.getCheckflag();
		Term term = null;
		if (checkReq.getCheckValue() == null || checkReq.getCheckValue().equals("")) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("check的值不能为空");
			return commonResponse;
		}
		if (flag == 0) {
			term = termMapper.findTermByZh(checkReq.getCheckValue());
		} else if (flag == 1) {
			term = termMapper.findTermByEn(checkReq.getCheckValue());
		} else if (flag == 2) {
			term = termMapper.findTermByField(checkReq.getCheckValue());
		} else {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("不支持check的类型");
			return commonResponse;
		}
		if (term != null) {
			commonResponse.setSuccess(false);
			commonResponse.setData(term);
			commonResponse.setMessage("已存在,请重新命名");
		}
		return commonResponse;
	}

	// 删除术语
	public CommonResponse deleteTerm(DeleteReq deleteReq) {
		CommonResponse commonResponse = new CommonResponse();
		if (termMapper.deleteTermById(deleteReq.getId()) != 1) {
			commonResponse.setMessage("删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	// 批量删除术语
	public CommonResponse bachDeleteTerm(BatchDeleteReq batchDeleteReq) {
		CommonResponse commonResponse = new CommonResponse();
		if(batchDeleteReq.getIds()==null||batchDeleteReq.getIds().length==0) {
			commonResponse.setMessage("请先选择要删除的行");
			commonResponse.setSuccess(false);
			return commonResponse;
		}
		if (termMapper.batchDeleteTerm(batchDeleteReq.getIds()) <= 0) {
			commonResponse.setMessage("删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	// 编辑术语
	public CommonResponse updateTerm(Term term) {
		CommonResponse commonResponse = new CommonResponse();
		Term oldTerm = termMapper.findTermById(term.getId());
		if(oldTerm==null){
			commonResponse.setSuccess(false);
			commonResponse.setMessage("术语不存在");
			return commonResponse;
		}
		//校验分类
		if(term.getClassify_id()!=oldTerm.getClassify_id()) {
			Classify classify = classifyMapper.findClassifyById(term.getClassify_id());
			if (classify == null) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("分类不存在");
				return commonResponse;
			}
		}
		try {
			if (termMapper.updateTerm(term) <=0) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("编辑术语失败，请稍后再试！");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			commonResponse.setSuccess(false);
			commonResponse.setMessage("编辑术语失败，请稍后再试！");
		}
		return commonResponse;
	}

	// 查询术语
	public CommonQueryResponse findTerm(TermQuery termQuery) {
		CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
		String condition = "";
		if (termQuery.getTerm_field() != null && (!termQuery.getTerm_field().equals(""))) {
			condition="and t.term_field like '"+termQuery.getTerm_field()+"%'";
		}
		if(termQuery.getZh_name() != null && (!termQuery.getZh_name().equals(""))){
			if(!condition.equals("")) {
				condition=condition+" ";
			}
			condition=condition+"and t.zh_name like '"+termQuery.getZh_name()+"%'";
		}
		if(termQuery.getClassify_id()>0){
			condition=condition+" and (one.id="+termQuery.getClassify_id()+" or two.id="+termQuery.getClassify_id()+" or three.id="+termQuery.getClassify_id()+")";
		}
		int page = termQuery.getPage();
		int size = termQuery.getSize();
		int startLine = (page - 1) * size;
		commonQueryResponse.setData(termMapper.queryTerm(condition, startLine, size));
		commonQueryResponse.setCurrentPage(termQuery.getPage());
		commonQueryResponse.setTotal(termMapper.queryTermCount(condition));
		return commonQueryResponse;
	}

	public CommonResponse analysisFile(MultipartFile file) {
		CommonResponse commonResponse = new CommonResponse();
		List<Term> termList = new ArrayList<>();
		try {
			CSVReader csvReader = new CSVReaderBuilder(
					new BufferedReader(
							new InputStreamReader(file.getInputStream(), "utf-8"))).build();
			Iterator<String[]> iterator = csvReader.iterator();
			while (iterator.hasNext()) {
				String[] next = iterator.next();
				Term term = new Term();
				if(next.length>=1){
					term.setZh_name(next[0]);
				}
				if(next.length>=2){
					term.setEn_name(next[1]);
				}
				if(next.length>=3){
					term.setTerm_field(next[1]);
				}
				if(next.length>=4){
					term.setDescr(next[1]);
				}
				termList.add(term);
			}
			commonResponse.setData(termList);
			return commonResponse;
		} catch (Exception e) {
			e.printStackTrace();
			commonResponse.setSuccess(false);
			commonResponse.setMessage(e.getMessage());
			return commonResponse;
		}
	}

	public CommonResponse batchAddTerm(BatchAddReq batchAddReq) {
		CommonResponse commonResponse = new CommonResponse();
		if(batchAddReq==null||batchAddReq.getTerms()==null||batchAddReq.getTerms().isEmpty()){
			commonResponse.setSuccess(false);
			commonResponse.setMessage("请求参数不能为空");
			return commonResponse;
		}
		//校验分类
		int classifyId = batchAddReq.getTerms().get(0).getClassify_id();
		Classify classify = classifyMapper.findClassifyById(classifyId);
		if(classify==null){
			commonResponse.setSuccess(false);
			commonResponse.setMessage("分类不存在");
			return commonResponse;
		}
		Set<String> zhNames=new HashSet<>();
		Set<String> enNames=new HashSet<>();
		Set<String> fields=new HashSet<>();
		//检查参数是是否重复
		for(Term term:batchAddReq.getTerms()){
			if(term==null || StringUtils.isEmpty(term.getZh_name()) || StringUtils.isEmpty(term.getTerm_field())|| StringUtils.isEmpty(term.getEn_name())){
				commonResponse.setSuccess(false);
				commonResponse.setMessage("中文名 英文名 编码不能为空");
				return commonResponse;
			}
			if(zhNames.contains(term.getZh_name())){
				commonResponse.setSuccess(false);
				commonResponse.setMessage("中文名为"+term.getZh_name()+" 存在多个");
				return commonResponse;
			}else{
				zhNames.add(term.getZh_name());
			}
			if(enNames.contains(term.getEn_name())){
				commonResponse.setSuccess(false);
				commonResponse.setMessage("英文名为"+term.getEn_name()+" 存在多个");
				return commonResponse;
			}else{
				enNames.add(term.getEn_name());
			}
			if(fields.contains(term.getTerm_field())){
				commonResponse.setSuccess(false);
				commonResponse.setMessage("英文简称为"+term.getTerm_field()+" 存在多个");
				return commonResponse;
			}else{
				fields.add(term.getTerm_field());
			}
			//判断分类
			if(term.getClassify_id()!=classifyId){
				if(classifyMapper.findClassifyById(term.getClassify_id())==null){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("分类不存在");
					return commonResponse;
				}
			}
			Term oldTerm = termMapper.findTerm(term.getZh_name(),term.getEn_name(),term.getTerm_field());
			if(oldTerm!=null){
				commonResponse.setSuccess(false);
				commonResponse.setMessage("与中文名为"+term.getZh_name()+" 的术语在中文,英文或英文简称上有重复");
				return commonResponse;
			}

		}
		//批量添加数据
		if(termMapper.batchInsertTerm(batchAddReq.getTerms())<1){
			commonResponse.setSuccess(false);
			commonResponse.setMessage("批量添加数据失败,请联系管理员");
			return commonResponse;
		}

		return commonResponse;
	}
}
