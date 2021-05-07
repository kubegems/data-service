package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacos.api.utils.StringUtils;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.AdjectiveQuery;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.AdjectiveMapper;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.AdjectiveTypeMapper;

@Service
public class AdjectiveService {
	@Autowired
	private AdjectiveMapper adjectiveMapper;
	@Autowired
	private AdjectiveTypeMapper adjectiveTypeMapper;

	public CommonResponse queryAllType() {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(adjectiveTypeMapper.findAllAdjectiveType());
		return commonResponse;
	}

	public CommonResponse deleteAdjective(DeleteReq deleteReq) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		if (adjectiveMapper.deleteAdjectiveById(deleteReq.getId()) <= 0) {
			commonResponse.setMessage("修饰词不存在或删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	public CommonResponse batchDeleteAdjective(BatchDeleteReq batchDeleteReq) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		if (batchDeleteReq.getIds() == null || batchDeleteReq.getIds().length == 0) {
			commonResponse.setMessage("删除的术语id不能为空");
			commonResponse.setSuccess(false);
			return commonResponse;
		}
		if (adjectiveMapper.batchDeleteAdjective(batchDeleteReq.getIds()) <= 0) {
			commonResponse.setMessage("修饰词不存在或删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	public CommonResponse checkUnique(CheckReq checkReq) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		byte flag = checkReq.getCheckflag();
		Adjective adjective = null;
		if (checkReq.getCheckValue() == null || checkReq.getCheckValue().equals("")) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("check的值不能为空");
			return commonResponse;
		}
		if (flag == 0) {
			adjective = adjectiveMapper.findAdjectiveByName(checkReq.getCheckValue());
		} else if (flag == 1) {
			adjective = adjectiveMapper.findAdjectiveByCode(checkReq.getCheckValue());
		} else if (flag == 2) {
			adjective = adjectiveMapper.findAdjectiveByCodeName(checkReq.getCheckValue());
		} else {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("不支持check的类型");
			return commonResponse;
		}
		if (adjective != null) {
			commonResponse.setSuccess(false);
			commonResponse.setData(adjective);
			commonResponse.setMessage("已存在,请重新命名");
		}
		return commonResponse;
	}

	public synchronized CommonResponse insertAdjective(Adjective adjective) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		// 基础校验
		if (adjective == null || StringUtils.isEmpty(adjective.getName())) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("名字不能为空");
			return commonResponse;
		}
		
		if (StringUtils.isEmpty(adjective.getCode())) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("编码不能为空");
			return commonResponse;
		}
		
		if (StringUtils.isEmpty(adjective.getCode_name())) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("编码简称不能为空");
			return commonResponse;
		}

		if (adjectiveMapper.findAdjectiveByName(adjective.getName()) != null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("名字已存在,请重新命名");
			return commonResponse;
		}
		
		if (adjectiveMapper.findAdjectiveByCode(adjective.getCode()) != null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("编码已存在,请重新命名");
			return commonResponse;
		}
		
		if (adjectiveMapper.findAdjectiveByCodeName(adjective.getCode_name()) != null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("编码简称已存在,请重新命名");
			return commonResponse;
		}
		// 插入数据库
		try {
			adjectiveMapper.insertAdjective(adjective);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			commonResponse.setSuccess(false);
			commonResponse.setMessage("数据插入失败,请稍后再试");
			return commonResponse;
		}
		return commonResponse;
	}

	public CommonQueryResponse queryAdjective(AdjectiveQuery adjectiveQuery) {
		// TODO Auto-generated method stub
		CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
		String condition = "deleted=0";
		if (adjectiveQuery.getType() != -1) {
			condition = condition + " and type=" + adjectiveQuery.getType();
		}

		if (adjectiveQuery.getName() != null && (!adjectiveQuery.getName().equals(""))) {
			condition = condition + " and name like '" + adjectiveQuery.getName() + "%'";
		}
		condition = condition + " order by update_time desc";
		int page = adjectiveQuery.getPage();
		int size = adjectiveQuery.getSize();
		int startLine = (page - 1) * size;
		commonQueryResponse.setData(adjectiveMapper.queryAdjective(condition, startLine, size));
		commonQueryResponse.setCurrentPage(adjectiveQuery.getPage());
		commonQueryResponse.setTotal(adjectiveMapper.queryAdjectiveCount(condition));
		return commonQueryResponse;
	}

	public CommonResponse updateAdjective(Adjective adjective) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		try {
			if (adjectiveMapper.updateAdjective(adjective) <=0) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("编辑修饰词失败，请稍后再试！");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			commonResponse.setSuccess(false);
			commonResponse.setMessage("编辑修饰词失败，请稍后再试！");
		}
		return commonResponse;
	}

}
