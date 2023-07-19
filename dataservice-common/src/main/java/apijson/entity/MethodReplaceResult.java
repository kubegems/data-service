package apijson.entity;

import lombok.Data;

@Data
public class MethodReplaceResult {
	private String sql;
	private boolean isColumnMap = false;
}
