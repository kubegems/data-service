package schedule.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class AtlasEntity implements Serializable{
	public enum Status { ACTIVE, DELETED }
	private static final long serialVersionUID = 1L;
	private String              typeName;
    private Map<String, Object> attributes;
    private String  guid           = null;
    private String  homeId         = null;
    private Boolean isProxy        = Boolean.FALSE;
    private Integer provenanceType = 0;
    private Status  status         = Status.ACTIVE;
    private String  createdBy      = null;
    private String  updatedBy      = null;
    private Date    createTime     = null;
    private Date    updateTime     = null;
    private Long    version        = 0L;
    
    public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public Map<String, Object> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, Object> attributes) {
		this.attributes = attributes;
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public String getHomeId() {
		return homeId;
	}

	public void setHomeId(String homeId) {
		this.homeId = homeId;
	}

	public Boolean getIsProxy() {
		return isProxy;
	}

	public void setIsProxy(Boolean isProxy) {
		this.isProxy = isProxy;
	}

	public Integer getProvenanceType() {
		return provenanceType;
	}

	public void setProvenanceType(Integer provenanceType) {
		this.provenanceType = provenanceType;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
}
