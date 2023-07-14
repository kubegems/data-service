package apijson.entity;

import lombok.Data;

@Data
public class DbInfo {
    private int id;
    private String db_url;
    private String db_name;
    private String userName;
    private String password;
    private String service_name;
    private String service_path;
    private int state;
    private String des;
    private int is_delete;
}
