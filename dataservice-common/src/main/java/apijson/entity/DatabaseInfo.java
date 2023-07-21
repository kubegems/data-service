package apijson.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class DatabaseInfo extends BaseEntity{
	private int id;
	private int db_id;
	private String database;
	private String service_path;
}
