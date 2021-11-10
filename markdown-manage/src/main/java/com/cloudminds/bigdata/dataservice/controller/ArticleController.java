package com.cloudminds.bigdata.dataservice.controller;
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.entity.Article;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;
import com.cloudminds.bigdata.dataservice.service.ArticleService;

@Controller
@RequestMapping("/article")
public class ArticleController {

    @Autowired
    private ArticleService articleService;

    @RequestMapping("/publish")
    @ResponseBody
    public String publishArticle(Article article) {
        boolean res = articleService.publishArticle(article);
        if(res) {
            return "success";
        }
        return "false";
    }


    @RequestMapping("/image/upload")
    @ResponseBody
    public JSONObject imageUpload(@RequestParam("editormd-image-file") MultipartFile image) {
        JSONObject jsonObject = new JSONObject();
        if(image != null) {
            String path = articleService.uploadFile(image);
            System.out.println(path);
            jsonObject.put("url", path);
            jsonObject.put("success", 1);
            jsonObject.put("message", "upload success!");
            return jsonObject;
        }
        jsonObject.put("success", 0);
        jsonObject.put("message", "upload error!");
        return jsonObject;
    }


    @RequestMapping("/get/{id}")
    public ModelAndView getArticleById(@PathVariable(name = "id")int id) {
        ModelAndView modelAndView = new ModelAndView();
        Article article = articleService.getArticleById(id);
        modelAndView.setViewName("article");
        if(article == null) {
            modelAndView.addObject("article", new Article());
        }
        modelAndView.addObject("article", article);
        return modelAndView;
    }

}
