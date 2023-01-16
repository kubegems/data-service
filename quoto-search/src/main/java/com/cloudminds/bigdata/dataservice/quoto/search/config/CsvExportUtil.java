package com.cloudminds.bigdata.dataservice.quoto.search.config;

import org.apache.commons.collections.CollectionUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

public class CsvExportUtil {
    /**
     * CSV文件列分隔符
     */
    private static final String CSV_COLUMN_SEPARATOR = ",";
    /**
     * CSV文件行分隔符
     */
    private static final String CSV_ROW_SEPARATOR = System.lineSeparator();

    /**
     * @param dataList 集合数据
     * @param titles   表头部数据
     * @param keys     表内容的键值
     * @param os       输出流
     */
    public static void doExport(List<Map<String, Object>> dataList, String titles, String keys, OutputStream os) throws Exception {
       // 保证线程安全
        StringBuffer buf = new StringBuffer();
        String[] titleArr = null;
        String[] keyArr = null;
        titleArr = titles.split(",");
        keyArr = keys.split(",");
        // 组装表头
        for (String title : titleArr) {
            buf.append(title).append(CSV_COLUMN_SEPARATOR);
        }
        buf.append(CSV_ROW_SEPARATOR);
        // 组装数据
        if (CollectionUtils.isNotEmpty(dataList)) {
            for (Map<String, Object> data : dataList) {
                for (String key : keyArr) {
                    buf.append("\t" + (data.get(key) != null ? data.get(key) + "" : "") + "\t").append(CSV_COLUMN_SEPARATOR);
                }
                buf.append(CSV_ROW_SEPARATOR);
            }
        }
        // 写出响应
        os.write(buf.toString().getBytes("GBK"));
        os.flush();
    }

    /**
     * 设置Header
     *
     * @param fileName
     * @param response
     * @throws UnsupportedEncodingException
     */
    public static void responseSetProperties(String fileName, HttpServletResponse response) throws UnsupportedEncodingException {
       // 设置文件后缀
        String fn = fileName + ".csv";
      // 读取字符编码
        String utf = "UTF-8";
        // 设置响应
        response.setContentType("application/force-download");
        response.setCharacterEncoding(utf);
        response.setHeader("Pragma", "public");
        response.setHeader("Cache-Control", "max-age=30");
        response.setHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(fn, utf));
    }
}
