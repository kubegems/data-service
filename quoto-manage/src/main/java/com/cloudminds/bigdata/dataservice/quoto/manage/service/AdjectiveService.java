package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Dimension;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Field;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.AdjectiveExtend;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.DimensionExtend;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.DimensionMapper;
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

@Service
public class AdjectiveService {
	@Autowired
	private AdjectiveMapper adjectiveMapper;
	@Autowired
	private DimensionMapper dimensionMapper;


	//删除修饰词
	public CommonResponse deleteAdjective(DeleteReq deleteReq) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		// 查询修饰词
		Adjective adjective = adjectiveMapper.findAdjectiveById(deleteReq.getId());
		if (adjective == null) {
			commonResponse.setMessage("id为" + deleteReq.getId() + "的修饰词不存在");
			commonResponse.setSuccess(false);
			return commonResponse;
		}
		// 判断修饰词有没有在用，在用不能删除
		List<String> quotoNames = adjectiveMapper.findQuotoNameByAdjectiveId(deleteReq.getId());
		if (quotoNames != null && quotoNames.size() > 0) {
			commonResponse.setMessage("指标" + quotoNames.toString() + "在使用(" + adjective.getName() + ")修饰词,不能删除");
			commonResponse.setSuccess(false);
			return commonResponse;
		}
		if (adjectiveMapper.deleteAdjectiveById(deleteReq.getId()) <= 0) {
			commonResponse.setMessage(adjective.getName() + "不存在或删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	//批量删除修饰词
	public CommonResponse batchDeleteAdjective(BatchDeleteReq batchDeleteReq) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		if (batchDeleteReq.getIds() == null || batchDeleteReq.getIds().length == 0) {
			commonResponse.setMessage("删除的修饰词id不能为空");
			commonResponse.setSuccess(false);
			return commonResponse;
		}
		for (int i = 0; i < batchDeleteReq.getIds().length; i++) {
			DeleteReq deleteReq = new DeleteReq();
			deleteReq.setId(batchDeleteReq.getIds()[i]);
			CommonResponse commonResponseDelete = deleteAdjective(deleteReq);
			if (!commonResponseDelete.isSuccess()) {
				return commonResponseDelete;
			}
		}
		return commonResponse;
	}

	//检测是否唯一
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

	//新增修饰词
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

		if(adjective.getType()==1){
			adjective.setDimension_id(0);
			adjective.setColumn_name(null);
			adjective.setReq_parm_type(2);
			adjective.setReq_parm(null);
		}else{
			if(StringUtils.isEmpty(adjective.getReq_parm())){
				commonResponse.setSuccess(false);
				commonResponse.setMessage("数据服务参数不能为空");
				return commonResponse;
			}
			if(adjective.getDimension_id()==0){
				if(StringUtils.isEmpty(adjective.getColumn_name())){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("没有关联维度的修饰词,列名不能为空");
					return commonResponse;
				}
			}else{
				if(dimensionMapper.queryDimensionById(adjective.getDimension_id())==null){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("维度不存在");
					return commonResponse;
				}
				adjective.setColumn_name(null);
			}
			if(adjective.getReq_parm_type()==1){
				Set<String> parameters = com.cloudminds.bigdata.dataservice.quoto.manage.utils.StringUtils.getParameterNames(adjective.getReq_parm());
				if(parameters.isEmpty()){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("值类型为变量的时候需要添加变量参数：${XXX}");
					return commonResponse;
				}
				if(adjective.getFields()==null||adjective.getFields().size()!=parameters.size()){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("参数说明的个数需和可变参数的数量相等");
					return commonResponse;
				}
				Set<String> fieldParameters = new HashSet<>();
				for(Field field:adjective.getFields()){
					fieldParameters.add(field.getName());
					if(!parameters.contains(field.getName())){
						commonResponse.setSuccess(false);
						commonResponse.setMessage("参数说明里的"+field.getName()+"并没有在数据服务参数里面定义");
						return commonResponse;
					}
					parameters.remove(field.getName());
				}
				if(parameters.size()!=0){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("这些可变参数"+parameters.toString()+"没有写参数说明");
					return commonResponse;
				}
			}
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

	//更新修饰词
	public CommonResponse updateAdjective(Adjective adjective) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		Adjective oldAdjective = adjectiveMapper.findAdjectiveById(adjective.getId());
		if(oldAdjective == null){
			commonResponse.setSuccess(false);
			commonResponse.setMessage("修饰类型不存在");
			return commonResponse;
		}
		// 基础校验
		if (StringUtils.isEmpty(adjective.getName())) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("名字不能为空");
			return commonResponse;
		}

		if (StringUtils.isEmpty(adjective.getCode())) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("编码不能为空");
			return commonResponse;
		}

		if(!oldAdjective.getName().equals(adjective.getName())) {
			if (adjectiveMapper.findAdjectiveByName(adjective.getName()) != null) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("名字已存在,请重新命名");
				return commonResponse;
			}
		}

		if(!oldAdjective.getCode().equals(adjective.getCode())) {
			if (adjectiveMapper.findAdjectiveByCode(adjective.getCode()) != null) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("编码已存在,请重新命名");
				return commonResponse;
			}
		}

		if(adjective.getType()==1){
			adjective.setDimension_id(0);
			adjective.setColumn_name(null);
			adjective.setReq_parm_type(2);
			adjective.setReq_parm(null);
		}else{
			if(StringUtils.isEmpty(adjective.getReq_parm())){
				commonResponse.setSuccess(false);
				commonResponse.setMessage("数据服务参数不能为空");
				return commonResponse;
			}
			if(adjective.getDimension_id()==0){
				if(StringUtils.isEmpty(adjective.getColumn_name())){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("没有关联维度的修饰词,列名不能为空");
					return commonResponse;
				}
			}else{
				if(dimensionMapper.queryDimensionById(adjective.getDimension_id())==null){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("维度不存在");
					return commonResponse;
				}
				adjective.setColumn_name(null);
			}
			if(adjective.getReq_parm_type()==1){
				Set<String> parameters = com.cloudminds.bigdata.dataservice.quoto.manage.utils.StringUtils.getParameterNames(adjective.getReq_parm());
				if(parameters.isEmpty()){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("值类型为变量的时候需要添加变量参数：${XXX}");
					return commonResponse;
				}
				if(adjective.getFields()==null||adjective.getFields().size()!=parameters.size()){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("参数说明的个数需和可变参数的数量相等");
					return commonResponse;
				}
				Set<String> fieldParameters = new HashSet<>();
				for(Field field:adjective.getFields()){
					fieldParameters.add(field.getName());
					if(!parameters.contains(field.getName())){
						commonResponse.setSuccess(false);
						commonResponse.setMessage("参数说明里的"+field.getName()+"并没有在数据服务参数里面定义");
						return commonResponse;
					}
					parameters.remove(field.getName());
				}
				if(parameters.size()!=0){
					commonResponse.setSuccess(false);
					commonResponse.setMessage("这些可变参数"+parameters.toString()+"没有写参数说明");
					return commonResponse;
				}
			}
		}

		// 更新数据库
		try {
			adjectiveMapper.updateAdjective(adjective);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			commonResponse.setSuccess(false);
			commonResponse.setMessage("数据更新失败,请稍后再试");
			return commonResponse;
		}
		return commonResponse;
	}

	//查询修饰词
	public CommonQueryResponse queryAdjective(AdjectiveQuery adjectiveQuery) {
		// TODO Auto-generated method stub
		CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
		String condition = "deleted=0";
		if (adjectiveQuery.getType() != -1) {
			condition = condition + " and type=" + adjectiveQuery.getType();
		}

		if(adjectiveQuery.getDimension_id()!=null){
			if(adjectiveQuery.getDimension_id()>0) {
				condition = condition + " and dimension_id=" + adjectiveQuery.getDimension_id();
			}else if(adjectiveQuery.getDimension_id()==-1){
				condition = condition + " and dimension_id is null";
			}
		}

		if (adjectiveQuery.getName() != null && (!adjectiveQuery.getName().equals(""))) {
			condition = condition + " and name like '" + adjectiveQuery.getName() + "%'";
		}
		condition = condition + " order by id asc";
		int page = adjectiveQuery.getPage();
		int size = adjectiveQuery.getSize();
		int startLine = (page - 1) * size;
		commonQueryResponse.setData(adjectiveMapper.queryAdjective(condition, startLine, size));
		commonQueryResponse.setCurrentPage(adjectiveQuery.getPage());
		commonQueryResponse.setTotal(adjectiveMapper.queryAdjectiveCount(condition));
		return commonQueryResponse;
	}

	//查询所有的修饰词
	public CommonResponse queryAllAdjective(AdjectiveQuery adjectiveQuery) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		String condition = "deleted=0";
		if (adjectiveQuery.getType() != -1) {
			condition = condition + " and type=" + adjectiveQuery.getType();
		}
		condition = condition + " order by id asc";
		commonResponse.setData(adjectiveMapper.queryAllAdjective(condition));
		return commonResponse;
	}

	//查询支持的修饰词
	public CommonResponse querySupportAdjective(int tableId) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		//查询支持的非时间的修饰词
		List<AdjectiveExtend> supportAdjective = new ArrayList<>();
		List<AdjectiveExtend> notTimeAdjective = adjectiveMapper.querySupportAdjective(tableId);
		if (notTimeAdjective != null && notTimeAdjective.size() > 0) {
			supportAdjective.addAll(notTimeAdjective);
		}

		//查询支持的时间修饰词
		List<ColumnAlias> timeColumnAlias = dimensionMapper.queryTimeColumnByTableId(tableId);
		if(timeColumnAlias!=null && timeColumnAlias.size()>0){
			List<AdjectiveExtend> timeAdjective = adjectiveMapper.queryTimeAdjective();
			if(timeAdjective!=null && timeAdjective.size()>0){
				supportAdjective.addAll(timeAdjective);
			}
		}
		commonResponse.setData(supportAdjective);
		return commonResponse;
	}

}
