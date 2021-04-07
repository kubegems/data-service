package com.cloudminds.bigdata.dataservice.standard.manage.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.TermQuery;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.TermMapper;

@Service
public class TermService {
	@Autowired
	private TermMapper termMapper;

	// 添加术语
	public CommonResponse insertTerm(Term term) {
		CommonResponse commonResponse = new CommonResponse();
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
			condition="and term_field like '"+termQuery.getTerm_field()+"%'";
		}
		if(termQuery.getZh_name() != null && (!termQuery.getZh_name().equals(""))){
			if(!condition.equals("")) {
				condition=condition+" ";
			}
			condition=condition+"and zh_name like '"+termQuery.getZh_name()+"%'";
		}
		int page = termQuery.getPage();
		int size = termQuery.getSize();
		int startLine = (page - 1) * size;
		commonQueryResponse.setData(termMapper.queryTerm(condition, startLine, size));
		commonQueryResponse.setCurrentPage(termQuery.getPage());
		commonQueryResponse.setTotal(termMapper.queryTermCount(condition));
		return commonQueryResponse;
	}
}
