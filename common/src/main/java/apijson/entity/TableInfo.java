package apijson.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class TableInfo extends BaseEntity{
	private int id;
	private int database_id;
	private String table_name;
	private String table_alias;
}
