package com.cloudminds.bigdata.dataservice.standard.manage.entity;
import lombok.Data;
import org.springframework.ldap.odm.annotations.Attribute;
import org.springframework.ldap.odm.annotations.DnAttribute;
import org.springframework.ldap.odm.annotations.Entry;
import org.springframework.ldap.odm.annotations.Id;

import javax.naming.Name;


@Data
@Entry(base = "ou=People,dc=cloudminds,dc=com", objectClasses = "account")
public class Account {
    @Id
    private Name id;
    @Attribute(name = "uid")
    private String uid;
    /* 用户姓名 */
    @Attribute(name = "cn")
    private String userName;
    /* 用户姓名 */
    @Attribute(name = "uidNumber")
    private String uidNumber;
    /* 用户邮箱 */
    @Attribute(name = "description")
    private String email;
}
