package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacos.api.utils.StringUtils;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Quoto;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.QuotoQuery;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.QuotoMapper;

@Service
public class QuotoService {
	@Autowired
	private QuotoMapper quotoMapper;

	public CommonResponse checkUnique(CheckReq checkReq) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		byte flag = checkReq.getCheckflag();
		Quoto quoto = null;
		if (checkReq.getCheckValue() == null || checkReq.getCheckValue().equals("")) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("check的值不能为空");
			return commonResponse;
		}
		if (flag == 0) {
			quoto = quotoMapper.findQuotoByName(checkReq.getCheckValue());
		} else if (flag == 1) {
			quoto = quotoMapper.findQuotoByField(checkReq.getCheckValue());
		} else {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("不支持check的类型");
			return commonResponse;
		}
		if (quoto != null) {
			commonResponse.setSuccess(false);
			commonResponse.setData(quoto);
			commonResponse.setMessage("已存在,请重新命名");
		}
		return commonResponse;
	}

	public CommonResponse queryAllBusiness() {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(quotoMapper.queryAllBusiness());
		return commonResponse;
	}

	public CommonResponse queryAllDataDomain(int businessId) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(quotoMapper.queryAllDataDomain(businessId));
		return commonResponse;
	}

	public CommonResponse queryAllBusinessProcess(int dataDomainId) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(quotoMapper.queryAllBusinessProcess(dataDomainId));
		return commonResponse;
	}

	public CommonResponse queryAllDataService() {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(quotoMapper.queryAllDataService());
		return commonResponse;
	}

	public CommonResponse queryAllDimension(int tableId) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(quotoMapper.queryAllDimension(tableId));
		return commonResponse;
	}

	public CommonResponse deleteQuoto(DeleteReq deleteReq) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		if (quotoMapper.deleteQuotoById(deleteReq.getId()) <= 0) {
			commonResponse.setMessage("指标不存在或删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	public CommonResponse batchDeleteQuoto(BatchDeleteReq batchDeleteReq) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		if (batchDeleteReq.getIds() == null || batchDeleteReq.getIds().length == 0) {
			commonResponse.setMessage("删除的指标id不能为空");
			commonResponse.setSuccess(false);
			return commonResponse;
		}
		if (quotoMapper.batchDeleteQuoto(batchDeleteReq.getIds()) <= 0) {
			commonResponse.setMessage("指标不存在或删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	public CommonResponse insertQuoto(Quoto quoto) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		// 基础校验
		if (quoto == null || StringUtils.isEmpty(quoto.getName())) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("名字不能为空");
			return commonResponse;
		}
		
		if (StringUtils.isEmpty(quoto.getField())) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("字段名称不能为空");
			return commonResponse;
		}
		

		if (quotoMapper.findQuotoByName(quoto.getName()) != null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("名字已存在,请重新命名");
			return commonResponse;
		}
		
		if (quotoMapper.findQuotoByField(quoto.getField()) != null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("字段已存在,请重新命名");
			return commonResponse;
		}
		
		// 插入数据库
		try {
			quotoMapper.insertQuoto(quoto);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			commonResponse.setSuccess(false);
			commonResponse.setMessage("数据插入失败,请稍后再试");
			return commonResponse;
		}
		return commonResponse;
	}

	public CommonResponse updateQuoto(Quoto quoto) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		try {
			if (quotoMapper.updateQuoto(quoto) <=0) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("编辑指标失败，请稍后再试！");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			commonResponse.setSuccess(false);
			commonResponse.setMessage("编辑指标失败，请稍后再试！");
		}
		return commonResponse;
	}

	public CommonQueryResponse queryQuoto(QuotoQuery quotoQuery) {
		// TODO Auto-generated method stub
		CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
		String condition = "deleted=0";
		if (quotoQuery.getType() != -1) {
			condition = condition + " and type=" + quotoQuery.getType();
		}

		if (quotoQuery.getName() != null && (!quotoQuery.getName().equals(""))) {
			condition = condition + " and name like '" + quotoQuery.getName() + "%'";
		}
		
		if (quotoQuery.getField() != null && (!quotoQuery.getField().equals(""))) {
			condition = condition + " and field like '" + quotoQuery.getField() + "%'";
		}
		
		condition = condition + " order by update_time desc";
		int page = quotoQuery.getPage();
		int size = quotoQuery.getSize();
		int startLine = (page - 1) * size;
		commonQueryResponse.setData(quotoMapper.queryQuoto(condition, startLine, size));
		commonQueryResponse.setCurrentPage(quotoQuery.getPage());
		commonQueryResponse.setTotal(quotoMapper.queryQuotoCount(condition));
		return commonQueryResponse;
	}

	public CommonResponse queryAllCycle() {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(quotoMapper.queryAllCycle());
		return commonResponse;
	}

	public CommonResponse queryQuotoById(int id) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		Quoto quoto=quotoMapper.queryQuotoById(id);
		if(quoto==null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("指标不存在,请核实id值是否正确");
			return commonResponse;
		}
		commonResponse.setData(quotoMapper.queryQuotoById(id));
		return commonResponse;
	}

}
