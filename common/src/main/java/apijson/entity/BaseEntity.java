package apijson.entity;

import lombok.Data;

@Data
public class BaseEntity {
	private int state;
	private int is_delete;
	private String des;
}
