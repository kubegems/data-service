package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.utils.StringUtils;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.DataServiceResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Quoto;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.QuotoInfo;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.ServicePathInfo;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums.StateEnum;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums.TypeEnum;
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
	@Value("${dataServiceUrl}")
	private String dataServiceUrl;
	@Autowired
	RestTemplate restTemplate;

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
		// 查询指标
		Quoto quoto = quotoMapper.findQuotoById(deleteReq.getId());
		if (quoto == null) {
			commonResponse.setMessage("id为：" + deleteReq.getId() + "的指标不存");
			commonResponse.setSuccess(false);
			return commonResponse;
		}
		// 指标是原始指标，是否有被派生指标引用
		if (quoto.getType() == TypeEnum.atomic_quoto.getCode()) {
			List<String> quotoNames = quotoMapper.findQuotoNameByOriginQuoto(deleteReq.getId());
			if (quotoNames != null && quotoNames.size() > 0) {
				commonResponse.setMessage("衍生指标" + quotoNames.toString() + "在使用(" + quoto.getName() + ")指标,不能删除");
				commonResponse.setSuccess(false);
				return commonResponse;
			}
		} else if (quoto.getType() == TypeEnum.derive_quoto.getCode()) { // 派生指标，是否有被复合指标引用

		}
		if (quotoMapper.deleteQuotoById(deleteReq.getId()) <= 0) {
			commonResponse.setMessage("指标(" + quoto.getName() + ")删除失败,请稍后再试");
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
		for (int i = 0; i < batchDeleteReq.getIds().length; i++) {
			DeleteReq deleteReq = new DeleteReq();
			deleteReq.setId(batchDeleteReq.getIds()[i]);
			CommonResponse commonResponseDelete = deleteQuoto(deleteReq);
			if (!commonResponseDelete.isSuccess()) {
				return commonResponseDelete;
			}
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
		if (quoto.getType() == 0) {
			if (quoto.getTable_id() == 0) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("数据服务不能为空");
				return commonResponse;
			}
			if (quoto.getCycle() == 0) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("计算周期不能为空");
				return commonResponse;
			}
		} else if (quoto.getType() == 1) {
			quoto.setState(StateEnum.active_state.getCode());
			if (quoto.getOrigin_quoto() <= 0) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("原子指标的id必须有值");
				return commonResponse;
			}
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
			if (quotoMapper.updateQuoto(quoto) <= 0) {
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

		if (quotoQuery.getBusinessId() != -1) {
			condition = condition + " and business_id=" + quotoQuery.getBusinessId();
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

	public CommonResponse queryAllQuoto(QuotoQuery quotoQuery) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		String condition = "deleted=0";
		if (quotoQuery.getType() != -1) {
			condition = condition + " and type=" + quotoQuery.getType();
		}

		condition = condition + " order by name asc";

		commonResponse.setData(quotoMapper.queryAllQuoto(condition));
		return commonResponse;
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
		Quoto quoto = quotoMapper.queryQuotoById(id);
		if (quoto == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("指标不存在,请核实id值是否正确");
			return commonResponse;
		}
		commonResponse.setData(quotoMapper.queryQuotoById(id));
		return commonResponse;
	}

	public CommonResponse activeQuoto(int id) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		Quoto quoto = quotoMapper.findQuotoById(id);
		if (quoto == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("指标不存在,请核实id值是否正确");
			return commonResponse;
		}
		if (quoto.getType() != TypeEnum.atomic_quoto.getCode()) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("只有原子指标才有激活的操作");
			return commonResponse;
		}
		if (quoto.getState() == StateEnum.active_state.getCode()) {
			commonResponse.setSuccess(true);
			commonResponse.setMessage("指标已激活");
			return commonResponse;
		}

		QuotoInfo quotoInfo = quotoMapper.queryQuotoInfo(quoto.getField());
		if (quotoInfo == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("数据服务没有此指标的配置,请前往数据服务-服务管理页配置此指标");
			return commonResponse;
		}
		if (quotoInfo.getState() == 0) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("数据服务此指标不可用,请前往数据服务-服务管理页启用此指标");
			return commonResponse;
		}

//		List<QuotoInfo> quotoInfo = quotoMapper.queryQuotoInfo(quoto.getField());
//		if (quotoInfo == null||quotoInfo.size()==0) {
//			commonResponse.setSuccess(false);
//			commonResponse.setMessage("数据服务没有此指标的配置,请前往数据服务-服务管理页配置此指标");
//			return commonResponse;
//		}
//		if (quotoInfo.get(0).getState() == 0) {
//			commonResponse.setSuccess(false);
//			commonResponse.setMessage("数据服务此指标不可用,请前往数据服务-服务管理页启用此指标");
//			return commonResponse;
//		}

		// 激活指标
		if (quotoMapper.updateQuotoState(StateEnum.active_state.getCode(), id) != 1) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("激活失败,请稍后再试");
			return commonResponse;
		}
		return commonResponse;
	}

	public CommonResponse queryFuzzy(QuotoQuery quotoQuery) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		String condition = "deleted=0";
		if (quotoQuery.getType() != -1) {
			condition = condition + " and type=" + quotoQuery.getType();
		}

		if (quotoQuery.getName() != null && (!quotoQuery.getName().equals(""))) {
			condition = condition + " and name like '" + quotoQuery.getName() + "%'";
		}
		commonResponse.setData(quotoMapper.queryQuotoFuzzy(condition));
		return commonResponse;
	}

	public CommonResponse queryQuotoData(Integer id, String quotoName, Integer page, Integer count) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		// 查询指标
		Quoto quoto = null;
		if (id != null && id > 0) {
			quoto = quotoMapper.queryQuotoById(id);
		}
		if (quoto == null && (!StringUtils.isEmpty(quotoName))) {
			quoto = quotoMapper.queryQuotoByName(quotoName);
		}
		if (page == null) {
			page = 0;
		}
		if (quoto == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("指标不存在");
			return commonResponse;
		}
		if (quoto.getState() != StateEnum.active_state.getCode()) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("非激活状态的指标不可以查询数据");
			return commonResponse;
		}
		if (count == null || count <= 0) {
			count = 1000;
		}

		if (count > 10000) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("count最大值为10000");
			return commonResponse;
		}
		return queryDataFromDataService(quoto, page, count);
	}

	public CommonResponse queryDataFromDataService(Quoto quoto, int page, int count) {
		CommonResponse commonResponse = new CommonResponse();

		// 复合指标处理逻辑
		if (quoto.getType() == TypeEnum.complex_quoto.getCode()) {
			return commonResponse;
		}
		// 非复合指标处理逻辑
		Quoto atomicQuoto = quoto;
		if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
			atomicQuoto = quotoMapper.findQuotoById(quoto.getOrigin_quoto());
		}
		// 查询数据服务对应的信息
		ServicePathInfo servicePathInfo = quotoMapper.queryServicePathInfo(atomicQuoto.getTable_id());
		if (servicePathInfo == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("指标对应的服务不可用,请联系管理员排查");
			return commonResponse;
		}
		String url = dataServiceUrl + servicePathInfo.getPath();
		String bodyRequest = "{'[]':{'" + servicePathInfo.getTableName() + "':{'@column':'" + atomicQuoto.getField();
		if (quoto.getType() == TypeEnum.derive_quoto.getCode()) {
			// 添加维度的请求参数
			if (quoto.getDimension() != null && quoto.getDimension().length > 0) {
				String group = "'@group':'";
				bodyRequest = bodyRequest + ";";
				// 查询维度的名称
				List<String> dimensionName = quotoMapper.queryDimensionName(quoto.getId());
				for (int i = 0; i < dimensionName.size(); i++) {
					if (i == dimensionName.size() - 1) {
						group = group + dimensionName.get(i) + "'";
						bodyRequest = bodyRequest + dimensionName.get(i) + "'";
					} else {
						group = group + dimensionName.get(i) + ",";
						bodyRequest = bodyRequest + dimensionName.get(i) + ";";
					}
				}
				bodyRequest = bodyRequest + "," + group;
			} else {
				bodyRequest = bodyRequest + "'";
			}
			// 添加修饰词的请求参数
			if (quoto.getAdjective() != null && quoto.getAdjective().length > 0) {
				// 查询修饰词信息
				List<Adjective> adjectives = quotoMapper.queryAdjective(quoto.getId());
				for (int i = 0; i < adjectives.size(); i++) {
					bodyRequest = bodyRequest + "," + getAdjectiveReq(adjectives.get(i));
				}
			}
		} else {
			bodyRequest = bodyRequest + "'";
		}
		bodyRequest = bodyRequest + "},'page':" + page + ",'count':" + count + "}}";
		System.out.println(bodyRequest);
		// 请求数据服务
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		// 将请求头部和参数合成一个请求
		HttpEntity<String> requestEntity = new HttpEntity<>(bodyRequest, headers);
		ResponseEntity<String> responseEntity = restTemplate.postForEntity(url, requestEntity, String.class);
		if (!responseEntity.getStatusCode().is2xxSuccessful()) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("指标对应的服务不可用,请联系管理员排查");
			return commonResponse;
		} else {
			JSONObject result = JSONObject.parseObject(responseEntity.getBody().toString());
			DataServiceResponse dataServiceResponse = JSONObject.toJavaObject(
					JSONObject.parseObject(responseEntity.getBody().toString()), DataServiceResponse.class);
			commonResponse.setSuccess(dataServiceResponse.isOk());
			commonResponse.setMessage(dataServiceResponse.getMsg());
			if (dataServiceResponse.isOk()) {
				if (result.get("[]") == null) {
					return commonResponse;
				}
				List<JSONObject> list = JSONObject.parseArray(result.get("[]").toString(), JSONObject.class);
				if (list != null) {
					if (list.size() == 1) {
						commonResponse.setData(list.get(0).get(servicePathInfo.getTableName()));
					} else {
						List<Object> data = new ArrayList<Object>();
						for (int i = 0; i < list.size(); i++) {
							data.add(list.get(i).get(servicePathInfo.getTableName()));
						}
						commonResponse.setData(data);
					}
				}
			}
		}
		return commonResponse;
	}

	public String getAdjectiveReq(Adjective adjective) {
		// 1为时间修饰词
		if (adjective.getType() == 1) {
			String result = "'" + adjective.getName();
			if (adjective.getCode_name().equals("last1HOUR")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
				result = result + ">=':'" + format.format(cal.getTime()) + "'";
			} else if (adjective.getCode_name().equals("last1DAY")) {
				result = result + ">=':'" + getLastDateByDay(0) + "'";
			} else if (adjective.getCode_name().equals("last2DAY")) {
				result = result + ">=':'" + getLastDateByDay(-1) + "'";
			} else if (adjective.getCode_name().equals("last3DAY")) {
				result = result + ">=':'" + getLastDateByDay(-2) + "'";
			} else if (adjective.getCode_name().equals("last7DAY") || adjective.getCode_name().equals("last1WEEK")) {
				result = result + ">=':'" + getLastDateByDay(-6) + "'";
			} else if (adjective.getCode_name().equals("last14DAY")) {
				result = result + ">=':'" + getLastDateByDay(-13) + "'";
			} else if (adjective.getCode_name().equals("last15DAY")) {
				result = result + ">=':'" + getLastDateByDay(-15) + "'";
			} else if (adjective.getCode_name().equals("last30DAY")) {
				result = result + ">=':'" + getLastDateByDay(-30) + "'";
			} else if (adjective.getCode_name().equals("last60DAY")) {
				result = result + ">=':'" + getLastDateByDay(-60) + "'";
			} else if (adjective.getCode_name().equals("last90DAY")) {
				result = result + ">=':'" + getLastDateByDay(-90) + "'";
			} else if (adjective.getCode_name().equals("last180DAY")) {
				result = result + ">=':'" + getLastDateByDay(-180) + "'";
			} else if (adjective.getCode_name().equals("last360DAY")) {
				result = result + ">=':'" + getLastDateByDay(-360) + "'";
			} else if (adjective.getCode_name().equals("last365DAY")) {
				result = result + ">=':'" + getLastDateByDay(-365) + "'";
			} else if (adjective.getCode_name().equals("last1MONTH")) {
				result = result + ">=':'" + getlastDateByMonth(-1) + "'";
			} else if (adjective.getCode_name().equals("last2MONTH")) {
				result = result + ">=':'" + getlastDateByMonth(-2) + "'";
			} else if (adjective.getCode_name().equals("last3MONTH")) {
				result = result + ">=':'" + getlastDateByMonth(-3) + "'";
			} else if (adjective.getCode_name().equals("last6MONTH")) {
				result = result + ">=':'" + getlastDateByMonth(-6) + "'";
			} else if (adjective.getCode_name().equals("last7MONTH")) {
				result = result + ">=':'" + getlastDateByMonth(-7) + "'";
			} else if (adjective.getCode_name().equals("last8MONTH")) {
				result = result + ">=':'" + getlastDateByMonth(-8) + "'";
			} else if (adjective.getCode_name().equals("last1YEAR")) {
				result = result + ">=':'" + getlastDateByYear(-1) + "'";
			} else if (adjective.getCode_name().equals("last2YEAR")) {
				result = result + ">=':'" + getlastDateByYear(-2) + "'";
			} else if (adjective.getCode_name().equals("ftDate(w)")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.WEEK_OF_MONTH, 0);
				cal.set(Calendar.DAY_OF_WEEK, 2);
				result = result + ">=':'" + format.format(cal.getTime()) + " 00:00:00'";
			} else if (adjective.getCode_name().equals("ftDate(m)")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.MONTH, 0);
				cal.set(Calendar.DAY_OF_MONTH, 1);
				result = result + ">=':'" + format.format(cal.getTime()) + " 00:00:00'";
			} else if (adjective.getCode_name().equals("ftDate(q)")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				Calendar cal = Calendar.getInstance();
				int currentMonth = cal.get(Calendar.MONTH) + 1;
				if (currentMonth >= 1 && currentMonth <= 3) {
					cal.set(Calendar.MONTH, 0);
				} else if (currentMonth >= 4 && currentMonth <= 6) {
					cal.set(Calendar.MONTH, 3);
				} else if (currentMonth >= 7 && currentMonth <= 9) {
					cal.set(Calendar.MONTH, 4);
				} else if (currentMonth >= 10 && currentMonth <= 12) {
					cal.set(Calendar.MONTH, 9);
				}
				cal.set(Calendar.DATE, 1);
				result = result + ">=':'" + format.format(cal.getTime()) + " 00:00:00'";
			} else if (adjective.getCode_name().equals("ftDate(y)")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy");
				Calendar cal = Calendar.getInstance();
				result = result + ">=':'" + format.format(cal.getTime()) + "-01-01" + " 00:00:00'";
			} else if (adjective.getCode_name().equals("pre1MONTH")) {
				result = result + "&{}':'>=\\'" + getPreDateByMonth(-1) + "\\',<\\'" + getPreDateByMonth(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre2MONTH")) {
				result = result + "&{}':'>=\\'" + getPreDateByMonth(-2) + "\\',<\\'" + getPreDateByMonth(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre3MONTH")) {
				result = result + "&{}':'>=\\'" + getPreDateByMonth(-3) + "\\',<\\'" + getPreDateByMonth(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre4MONTH")) {
				result = result + "&{}':'>=\\'" + getPreDateByMonth(-4) + "\\',<\\'" + getPreDateByMonth(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre5MONTH")) {
				result = result + "&{}':'>=\\'" + getPreDateByMonth(-5) + "\\',<\\'" + getPreDateByMonth(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre6MONTH")) {
				result = result + "&{}':'>=\\'" + getPreDateByMonth(-6) + "\\',<\\'" + getPreDateByMonth(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre12MONTH")) {
				result = result + "&{}':'>=\\'" + getPreDateByMonth(-12) + "\\',<\\'" + getPreDateByMonth(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre24MONTH")) {
				result = result + "&{}':'>=\\'" + getPreDateByMonth(-24) + "\\',<\\'" + getPreDateByMonth(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre1DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-1) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre2DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-2) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre3DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-3) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre7DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-7) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre14DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-14) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre15DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-15) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre30DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-30) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre60DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-60) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre90DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-90) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre180DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-180) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre360DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-360) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre365DAY")) {
				result = result + "&{}':'>=\\'" + getLastDateByDay(-365) + "\\',<\\'" + getLastDateByDay(0) + "\\''";
			} else if (adjective.getCode_name().equals("pre1YEAR")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy");
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.YEAR, -1);
				Calendar calEnd = Calendar.getInstance();
				result = result + "&{}':'>=\\'" + format.format(cal.getTime()) + "-01-01 00:00:00\\',<\\'"
						+ format.format(calEnd.getTime()) + "-01-01 00:00:00\\''";
			} else if (adjective.getCode_name().equals("pre1QUARTER")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				Calendar cal = Calendar.getInstance();
				int currentMonth = cal.get(Calendar.MONTH) + 1;
				if (currentMonth >= 1 && currentMonth <= 3) {
					cal.set(Calendar.MONTH, 0);
				} else if (currentMonth >= 4 && currentMonth <= 6) {
					cal.set(Calendar.MONTH, 3);
				} else if (currentMonth >= 7 && currentMonth <= 9) {
					cal.set(Calendar.MONTH, 4);
				} else if (currentMonth >= 10 && currentMonth <= 12) {
					cal.set(Calendar.MONTH, 9);
				}
				cal.set(Calendar.DATE, 1);
				String end = format.format(cal.getTime()) + " 00:00:00";
				cal.add(Calendar.MONTH, -3);
				String start = format.format(cal.getTime()) + " 00:00:00";
				result = result + "&{}':'>=\\'" + start + "\\',<\\'" + end + "\\''";
			}
			return result;
		}

		// 其它修饰词
		return "'" + adjective.getName() + adjective.getCode().substring(1);
	}

	public String getLastDateByDay(int day) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		return format.format(cal.getTime()) + " 00:00:00";
	}

	public String getlastDateByMonth(int month) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, month);
		cal.add(Calendar.DATE, 1);
		return format.format(cal.getTime()) + " 00:00:00";
	}

	public String getlastDateByYear(int year) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.YEAR, year);
		cal.add(Calendar.DATE, 1);
		return format.format(cal.getTime()) + " 00:00:00";
	}

	public String getPreDateByMonth(int month) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, month);
		return format.format(cal.getTime()) + "-01 00:00:00";
	}

}
