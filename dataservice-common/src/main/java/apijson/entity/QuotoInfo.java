package apijson.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class QuotoInfo extends BaseEntity{
	private int id;
	private int table_id;
	private String quoto_name;
	private String quoto_sql;
}
