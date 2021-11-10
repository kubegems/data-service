package com.cloudminds.bigdata.dataservice.service;


import com.cloudminds.bigdata.dataservice.entity.Article;
import com.cloudminds.bigdata.dataservice.mapper.ArticleMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

@Service
public class ArticleService {

    @Value("${spring.servlet.multipart.location}")
    private String markdownFile;

    @Autowired
    private ArticleMapper articleMapper;

    public boolean publishArticle(Article article) {
        int res = articleMapper.insertArticle(article);
        if (res > 0) {
            return true;
        }
        return false;
    }


    public Article getArticleById(int id) {
        return articleMapper.getArticleById(id);
    }

    public  String uploadFile(MultipartFile file) {
        if(file.isEmpty()) {
            return "";
        }
        // 获取原文件名
        String originFileName = file.getOriginalFilename();
        // 我们通过UUID 来重新重组文件名
        String uid = UUID.randomUUID().toString();
        assert originFileName != null;
        String suffix = originFileName.substring(originFileName.lastIndexOf('.') + 1);
        String path = markdownFile+"/upload/" + uid + "." + suffix;
        String returnPath = "/upload/" + uid + "." + suffix;
        File newFile = new File(path);
        if(newFile.getParentFile() != null && !newFile.getParentFile().exists()) {
            System.out.println("创建目录ing");
            // 上面的 newFile.getParentFile() 已经保证了不为null.
            if(newFile.getParentFile().mkdirs()) {
                System.out.println("创建目录成功");
            }else {
                System.out.println("创建目录失败");
                return "";
            }
        }
        try {
            file.transferTo(newFile);
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
        return returnPath;
    }

}
