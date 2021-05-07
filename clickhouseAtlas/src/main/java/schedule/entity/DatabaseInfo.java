package schedule.entity;

import lombok.Data;

@Data
public class DatabaseInfo {
	private String name;
	private String engine;
	private String data_path;
	private String metadata_path;
	private float diskSize;
	private float originSize;
	
}
