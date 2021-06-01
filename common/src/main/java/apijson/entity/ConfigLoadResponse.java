package apijson.entity;

import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConfigLoadResponse extends CommonResponse{
	Map<String, String> TABLE_KEY_MAP;
	Map<String, Map<String, String>> tableColumnMap;
}
