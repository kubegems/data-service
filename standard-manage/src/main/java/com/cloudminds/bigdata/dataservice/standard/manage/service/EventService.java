package com.cloudminds.bigdata.dataservice.standard.manage.service;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacos.api.utils.StringUtils;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.EventInfo;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.enums.StateEnum;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.EventBatchDeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.EventDeleteReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.EventQuery;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.request.ReviewReq;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.standard.manage.mapper.EventMapper;

@Service
public class EventService {
	@Autowired
	private EventMapper eventMapper;

	// 增加事件
	public synchronized CommonResponse insertEvent(EventInfo eventInfo) {
		CommonResponse commonResponse = new CommonResponse();
		// 基础校验
		if (eventInfo == null || StringUtils.isEmpty(eventInfo.getEvent_name())) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("事件名不能为空");
			return commonResponse;
		}

		if (eventMapper.findEventByEventName(eventInfo.getEvent_name()) != null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("名字已存在,请重新命名");
			return commonResponse;
		}
		// 生成版本号
		eventInfo.setVersion(new SimpleDateFormat("yyyy.MM.dd").format(new Date()));
		// 生成事件编码
		String eventCode = eventMapper.findMaxEventCode();
		if (StringUtils.isEmpty(eventCode)) {
			eventCode = "000001";
		} else {
			eventCode = (Integer.parseInt(eventCode) + 1) + "";
			while (eventCode.length() < 6) {
				eventCode = "0" + eventCode;
			}
		}
		eventInfo.setEvent_code(eventCode);
		eventInfo.setState(StateEnum.develop_state.getCode());
		// 插入数据库
		try {
			eventMapper.insertEvent(eventInfo);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			commonResponse.setSuccess(false);
			commonResponse.setMessage("数据插入失败,请稍后再试");
			return commonResponse;
		}
		return commonResponse;
	}

	// 检查是否唯一
	public CommonResponse checkUnique(CheckReq checkReq) {
		CommonResponse commonResponse = new CommonResponse();
		if (checkReq.getCheckValue() == null || checkReq.getCheckValue().equals("")) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("check的值不能为空");
			return commonResponse;
		}
		if (eventMapper.findEventByEventName(checkReq.getCheckValue()) != null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("名字已存在,请重新命名");
		}
		return commonResponse;
	}

	// 查询事件
	public CommonQueryResponse queryEvent(EventQuery eventQuery) {
		CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
		String condition = "and state<5";
		if (eventQuery.getModel_name() != null && (!eventQuery.getModel_name().equals(""))) {
			condition = condition + " and model_name='" + eventQuery.getModel_name() + "'";
		}

		if (eventQuery.getEvent_name() != null && (!eventQuery.getEvent_name().equals(""))) {
			condition = condition + " and event_name like '" + eventQuery.getEvent_name() + "%'";
		}

		if (eventQuery.getEvent_code() != null && (!eventQuery.getEvent_code().equals(""))) {
			condition = condition + " and event_code like '" + eventQuery.getEvent_code() + "%'";
		}

		if (eventQuery.getType() != -1) {
			condition = condition + " and type=" + eventQuery.getType();
		}
		condition = condition + " order by update_time desc";
		int page = eventQuery.getPage();
		int size = eventQuery.getSize();
		int startLine = (page - 1) * size;
		commonQueryResponse.setData(eventMapper.queryEvent(condition, startLine, size));
		commonQueryResponse.setCurrentPage(eventQuery.getPage());
		commonQueryResponse.setTotal(eventMapper.queryEventCount(condition));
		return commonQueryResponse;
	}

	// 查询审核页的事件
	public CommonQueryResponse queryReviewEvent(EventQuery eventQuery) {
		CommonQueryResponse commonQueryResponse = new CommonQueryResponse();
		String condition = "";
		if (eventQuery.getState() == -1) {
			condition = "and state>0";
		} else if (eventQuery.getState() == StateEnum.publish_state.getCode()) {
			condition = "and (state=" + StateEnum.publish_state.getCode() + " or state="
					+ StateEnum.oldpublish_state.getCode() + ")";
		} else if (eventQuery.getState() == StateEnum.offline_state.getCode()) {
			condition = "and (state=" + StateEnum.offline_state.getCode() + " or state="
					+ StateEnum.oldoffline_state.getCode() + ")";
		} else {
			condition = "and state=" + eventQuery.getState();
		}
		if (eventQuery.getModel_name() != null && (!eventQuery.getModel_name().equals(""))) {
			condition = condition + " and model_name='" + eventQuery.getModel_name() + "'";
		}

		if (eventQuery.getEvent_name() != null && (!eventQuery.getEvent_name().equals(""))) {
			condition = condition + " and event_name like '" + eventQuery.getEvent_name() + "%'";
		}

		if (eventQuery.getEvent_code() != null && (!eventQuery.getEvent_code().equals(""))) {
			condition = condition + " and event_code like '" + eventQuery.getEvent_code() + "%'";
		}

		if (eventQuery.getType() != -1) {
			condition = condition + " and type=" + eventQuery.getType();
		}

		String conditionOrder = condition;
		if (eventQuery.getState() == -1) {
			conditionOrder = conditionOrder + " order by state asc";
		} else {
			conditionOrder = conditionOrder + " order by update_time desc";
		}

		int page = eventQuery.getPage();
		int size = eventQuery.getSize();
		int startLine = (page - 1) * size;
		commonQueryResponse.setData(eventMapper.queryEvent(conditionOrder, startLine, size));
		commonQueryResponse.setCurrentPage(eventQuery.getPage());
		commonQueryResponse.setTotal(eventMapper.queryEventCount(condition));
		return commonQueryResponse;
	}

	// 查询事件详情
	public CommonResponse queryById(int id) {
		CommonResponse commonResponse = new CommonResponse();
		EventInfo eventInfo = eventMapper.queryEventById(id);
		if (eventInfo == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("事件不存在");
		}
		commonResponse.setData(eventMapper.queryEventById(id));
		return commonResponse;
	}

	// 查询所有的版本信息
	public CommonResponse queryAllVersion(String event_code) {
		CommonResponse commonResponse = new CommonResponse();
		if (StringUtils.isBlank(event_code)) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("时间编码不能为空");
			return commonResponse;
		}
		commonResponse.setData(eventMapper.queryVersionInfo(event_code));
		return commonResponse;
	}

	// 删除事件
	public CommonResponse deleteEvent(EventDeleteReq deleteReq) {
		CommonResponse commonResponse = new CommonResponse();
		if (eventMapper.deleteEventById(deleteReq.getEvent_code()) <= 0) {
			commonResponse.setMessage("事件不存在或删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	// 批量删除事件
	public CommonResponse batchDeleteEvent(EventBatchDeleteReq batchDeleteReq) {
		CommonResponse commonResponse = new CommonResponse();
		if (batchDeleteReq.getEvent_codes() == null || batchDeleteReq.getEvent_codes().length == 0) {
			commonResponse.setMessage("删除的术语id不能为空");
			commonResponse.setSuccess(false);
			return commonResponse;
		}
		if (eventMapper.batchDeleteEvent(batchDeleteReq.getEvent_codes()) <= 0) {
			commonResponse.setMessage("事件不存在或删除失败,请稍后再试");
			commonResponse.setSuccess(false);
		}
		return commonResponse;
	}

	// 发布事件
	public CommonResponse onlineEvent(DeleteReq onlineReq) {
		CommonResponse commonResponse = new CommonResponse();
		EventInfo eventInfo = eventMapper.queryEventById(onlineReq.getId());
		if (eventInfo == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("事件不存在");
			return commonResponse;
		}
		if (eventInfo.getState() == StateEnum.publish_state.getCode()) {
			commonResponse.setMessage("事件已是发布状态");
			return commonResponse;
		}
		EventInfo eventInfoOld = eventMapper.findOtherPublishEventById(onlineReq.getId(), eventInfo.getEvent_code());
		if (eventInfoOld != null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("事件只能有一个版本发布,请下线版本：" + eventInfoOld.getVersion() + ",在做发布操作");
			return commonResponse;
		}
		if (eventInfo.getState() == StateEnum.pass_state.getCode()
				|| eventInfo.getState() == StateEnum.offline_state.getCode()) {
			if (eventMapper.updateEventState(onlineReq.getId(), StateEnum.publish_state.getCode()) != 1) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("发布失败,请稍后再试");
			}
		} else if (eventInfo.getState() == StateEnum.oldoffline_state.getCode()) {
			if (eventMapper.updateEventState(onlineReq.getId(), StateEnum.oldpublish_state.getCode()) != 1) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("发布失败,请稍后再试");
			}
		} else {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("此状态不支持发布");
		}
		return commonResponse;
	}

	// 下线事件
	public CommonResponse offlineEvent(DeleteReq offlineReq) {
		CommonResponse commonResponse = new CommonResponse();
		EventInfo eventInfo = eventMapper.queryEventById(offlineReq.getId());
		if (eventInfo == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("事件不存在");
			return commonResponse;
		}
		if (eventInfo.getState() == StateEnum.offline_state.getCode()
				|| eventInfo.getState() == StateEnum.oldoffline_state.getCode()) {
			commonResponse.setMessage("事件已是下线状态");
			return commonResponse;
		}
		if (eventInfo.getState() == StateEnum.publish_state.getCode()) {
			if (eventMapper.updateEventState(offlineReq.getId(), StateEnum.offline_state.getCode()) != 1) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("下线失败,请稍后再试");
			}
		} else if (eventInfo.getState() == StateEnum.oldpublish_state.getCode()) {
			if (eventMapper.updateEventState(offlineReq.getId(), StateEnum.oldoffline_state.getCode()) != 1) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("下线失败,请稍后再试");
			}
		} else {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("此状态不支持下线");
		}
		return commonResponse;
	}

	// 申请审核
	public CommonResponse applyReview(DeleteReq applyReq) {
		CommonResponse commonResponse = new CommonResponse();
		EventInfo eventInfo = eventMapper.queryEventById(applyReq.getId());
		if (eventInfo == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("事件不存在");
			return commonResponse;
		}
		if (eventInfo.getState() == StateEnum.wait_state.getCode()) {
			commonResponse.setMessage("事件已是等待审核状态");
			return commonResponse;
		}
		if (eventInfo.getState() == StateEnum.develop_state.getCode()) {
			if (eventMapper.updateEventState(applyReq.getId(), StateEnum.wait_state.getCode()) != 1) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("申请失败,请稍后再试");
			}
		} else {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("此状态不支持申请审核");
		}
		return commonResponse;
	}

	// 查询事件数量信息
	public CommonResponse queryEventNumInfo() {
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(eventMapper.queryEventNumInfo());
		return commonResponse;
	}

	// 查询各模块的数量信息
	public CommonResponse queryModelEventNumInfo(int type) {
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(eventMapper.queryModelEventNumInfo(type));
		return commonResponse;
	}

	// 审核
	public CommonResponse review(ReviewReq reviewReq) {
		CommonResponse commonResponse = new CommonResponse();
		EventInfo eventInfo = eventMapper.queryEventById(reviewReq.getId());
		if (eventInfo == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("事件不存在");
			return commonResponse;
		}
		if (eventInfo.getState() != StateEnum.wait_state.getCode()) {
			commonResponse.setMessage("此状态的事件不能被审核");
			return commonResponse;
		}
		if (reviewReq.isPass()) {
			if (eventMapper.updateEventReviewResult(reviewReq.getId(), StateEnum.pass_state.getCode(), "") != 1) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("审核失败,请稍后再试");
			}
		} else {
			if (StringUtils.isBlank(reviewReq.getMessage())) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("不通过时,意见不能为空");
			} else {
				if (eventMapper.updateEventReviewResult(reviewReq.getId(), StateEnum.develop_state.getCode(),
						reviewReq.getMessage()) != 1) {
					commonResponse.setSuccess(false);
					commonResponse.setMessage("审核失败,请稍后再试");
				}
			}
		}
		return commonResponse;
	}

	public CommonResponse updateEvent(EventInfo eventInfo) {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		EventInfo eventOldInfo = eventMapper.queryEventById(eventInfo.getId());
		// 事件不存在
		if (eventOldInfo == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("事件不存在");
			return commonResponse;
		}
		// 等待审核的事件不能编辑
		if (eventOldInfo.getState() == StateEnum.wait_state.getCode()) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("等待审核的事件不能编辑");
			return commonResponse;
		}
		// 老版本的事件只能更新描述 or 审核通过,已上线,已下线,改描述 (state为0 没有改变字段 state为1 改变了字段)
		if (eventOldInfo.getState() == StateEnum.oldoffline_state.getCode()
				|| eventOldInfo.getState() == StateEnum.oldpublish_state.getCode()
				|| ((eventOldInfo.getState() == StateEnum.pass_state.getCode()
						|| eventOldInfo.getState() == StateEnum.publish_state.getCode()
						|| eventOldInfo.getState() == StateEnum.offline_state.getCode())
						&& (!eventInfo.isChangeFields()))) {
			eventOldInfo.setDescr(eventInfo.getDescr());
			eventOldInfo.setJira_num(eventInfo.getJira_num());
			if (eventMapper.updateEvent(eventOldInfo) != 1) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("事件更新失败,请稍后再试");
				return commonResponse;
			}
			commonResponse.setMessage("描述更新成功");
			return commonResponse;
		}

		// 审核通过后修改了字段，状态变为开发中
		if (eventOldInfo.getState() == StateEnum.pass_state.getCode() && eventInfo.isChangeFields()) {
			eventOldInfo.setDescr(eventInfo.getDescr());
			eventOldInfo.setJira_num(eventInfo.getJira_num());
			eventOldInfo.setFields(eventInfo.getFields());
			eventOldInfo.setState(StateEnum.develop_state.getCode());
			if (eventMapper.updateEvent(eventOldInfo) != 1) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("事件更新失败,请稍后再试");
				return commonResponse;
			}
			commonResponse.setMessage("事件更新成功,状态变为:" + StateEnum.develop_state.getDesc());
			return commonResponse;
		}

		// 开发中变更 state为0 表示没有历史版本 state为1有历史版本
		if (eventOldInfo.getState() == StateEnum.develop_state.getCode()) {
			if (eventInfo.isUniqueVersion()) {
				if (!eventOldInfo.getEvent_name().equals(eventInfo.getEvent_name())) {
					if (eventMapper.findEventByEventName(eventInfo.getEvent_name()) != null) {
						commonResponse.setSuccess(false);
						commonResponse.setMessage("名字已存在,请重新命名");
						return commonResponse;
					}
					eventOldInfo.setEvent_name(eventInfo.getEvent_name());
				}
				eventOldInfo.setDescr(eventInfo.getDescr());
				eventOldInfo.setFields(eventInfo.getFields());
				eventOldInfo.setJira_num(eventInfo.getJira_num());
				eventOldInfo.setModel_name(eventInfo.getModel_name());
				eventOldInfo.setModel_version(eventInfo.getModel_version());
				if (eventMapper.updateEvent(eventOldInfo) != 1) {
					commonResponse.setSuccess(false);
					commonResponse.setMessage("事件更新失败,请稍后再试");
					return commonResponse;
				}
				commonResponse.setMessage("事件更新成功");
				return commonResponse;
			} else {
				eventOldInfo.setDescr(eventInfo.getDescr());
				eventOldInfo.setFields(eventInfo.getFields());
				eventOldInfo.setJira_num(eventInfo.getJira_num());
				if (eventMapper.updateEvent(eventOldInfo) != 1) {
					commonResponse.setSuccess(false);
					commonResponse.setMessage("事件更新失败,请稍后再试");
					return commonResponse;
				}
				commonResponse.setMessage("事件更新成功");
				return commonResponse;
			}
		}

		// 事件发布和下线状态需更改字段 新增一条记录
		if (eventOldInfo.getState() == StateEnum.publish_state.getCode()
				|| eventOldInfo.getState() == StateEnum.offline_state.getCode()) {
			// 新增记录
			// 生成版本号
			String version = new SimpleDateFormat("yyyy.MM.dd").format(new Date());
			if (version.equals(eventOldInfo.getVersion())) {
				commonResponse.setSuccess(false);
				commonResponse.setMessage("当天创建并上过线的版本不能更改字段信息");
				return commonResponse;
			}
			eventOldInfo.setVersion(version);
			eventOldInfo.setDescr(eventInfo.getDescr());
			eventOldInfo.setJira_num(eventInfo.getJira_num());
			eventOldInfo.setCreator(eventInfo.getCreator());
			eventOldInfo.setFields(eventInfo.getFields());
			// 插入数据库
			try {
				eventMapper.insertEvent(eventOldInfo);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				commonResponse.setSuccess(false);
				commonResponse.setMessage("事件更新失败");
				return commonResponse;
			}
			// 更新历史事件状态
			int state = StateEnum.oldoffline_state.getCode();
			if (eventOldInfo.getState() == StateEnum.publish_state.getCode()) {
				state = StateEnum.oldpublish_state.getCode();
			}
			eventMapper.updateEventState(eventInfo.getId(), state);
			commonResponse.setData(eventOldInfo.getId());
			return commonResponse;
		}
		commonResponse.setSuccess(false);
		commonResponse.setMessage("此状态不支持修改");
		return commonResponse;
	}

}
