package com.cloudminds.bigdata.dataservice.mapper;

import com.cloudminds.bigdata.dataservice.entity.Article;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface ArticleMapper {
    @Insert("insert into article(title,content,creator,descr,create_time,update_time) values(#{title},#{content},#{creator},#{descr},now(),now())")
    public int insertArticle(Article article);

    @Select("select * from article where id=#{id} and deleted=0")
    public Article getArticleById(int id);
}
