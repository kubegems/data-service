package com.cloudminds.bigdata.dataservice.quoto.manage.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
    /**
     * 获取表达式中${}中的值
     * @param content
     * @return
     */
    public static Set<String> getParameterNames(String content) {
        Pattern regex = Pattern.compile("\\$\\{([^}]*)\\}");
        Matcher matcher = regex.matcher(content);
        Set<String> result = new HashSet<>();
        while(matcher.find()) {
            result.add(matcher.group(1));
        }
        return result;
    }
}
