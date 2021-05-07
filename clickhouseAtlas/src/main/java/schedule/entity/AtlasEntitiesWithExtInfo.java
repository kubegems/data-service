package schedule.entity;

import java.io.Serializable;
import java.util.List;

public class AtlasEntitiesWithExtInfo implements Serializable{
	private static final long serialVersionUID = 1L;
	private List<AtlasEntity> entities;
	public List<AtlasEntity> getEntities() {
		return entities;
	}
	public void setEntities(List<AtlasEntity> entities) {
		this.entities = entities;
	}
}
