package apijson.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class TableAlias extends BaseEntity{
	private int id;
	private int table_id;
	private String table_alias;
}
