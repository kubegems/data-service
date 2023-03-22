package com.cloudminds.bigdata.dataservice.quoto.config.amazons3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@SuppressWarnings(value = "all")
public class OssUtils {

    private static AmazonS3 amazonS3;

    @Autowired
    public OssUtils(AmazonS3 amazonS3){
        OssUtils.amazonS3 = amazonS3;
    }

    /**
     * 获取所有bucket
     * @return bucket列表
     */
    public static List<Bucket> bucketList(){
        return amazonS3.listBuckets();
    }

    /**
     * 删除bucket
     * @param bucketName bucketName
     */
    public static void deleteBucket(String bucketName){
        amazonS3.deleteBucket(bucketName);
    }

    //============================ 文件相关 ==================================//

    /**
     * 上传文件
     * @param bucketName bucketName
     * @param fileName fileName
     * @param file file
     * @return 文件信息
     */
    public static PutObjectResult uploadFile(String bucketName, String fileName, File file){
        return amazonS3.putObject(new PutObjectRequest(bucketName, fileName, file).withCannedAcl(CannedAccessControlList.PublicReadWrite));
    }

    /**
     * 上传文件
     * @param bucketName bucketName
     * @param fileName fileName
     * @param inputStream 文件流
     * @return 文件信息
     */
    public static PutObjectResult uploadFile(String bucketName, String fileName, InputStream inputStream){
        ObjectMetadata objectMetadata = new ObjectMetadata();
        try {
            byte[] bytes = IOUtils.toByteArray(inputStream);
            objectMetadata.setContentLength(inputStream.available());
            objectMetadata.setContentType("application/octet-stream");
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        }catch (Exception e){
            log.error("上传文件失败，" + e);
        }
        return amazonS3.putObject(bucketName, fileName, inputStream, objectMetadata);
    }

    /**
     * 批量删除文件
     * @param bucketName bucketName
     * @param fileNames 文件列表
     */
    public static void deleteFile(String bucketName, String... fileNames){
        List<DeleteObjectsRequest.KeyVersion> collect = Lists.newArrayList(fileNames).stream()
                .map(DeleteObjectsRequest.KeyVersion::new)
                .collect(Collectors.toList());
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName);
        deleteObjectsRequest.setKeys(collect);
        amazonS3.deleteObjects(deleteObjectsRequest);
    }

    /**
     * 获取文件
     * @param bucketName bucketName
     * @param fileName fileName
     * @return 文件
     */
    public static S3Object getFile(String bucketName, String fileName){
        return amazonS3.getObject(bucketName, fileName);
    }

    /**
     * 批量获取文件信息
     * @param bucketName bucketName
     * @param prefix prefix
     * @return 文件信息列表
     */
    public static List<S3ObjectSummary> getFileList(String bucketName, String prefix){
        ListObjectsV2Result listObjectsV2Result;
        if (StringUtils.isBlank(prefix)){
            listObjectsV2Result = amazonS3.listObjectsV2(bucketName);
        }else {
            listObjectsV2Result = amazonS3.listObjectsV2(bucketName, prefix);
        }
        return listObjectsV2Result.getObjectSummaries();
    }

    /**
     * 获取文件外链
     * @param bucketName bucketName
     * @param filename filename
     * @param expire 过期时间
     * @return url
     */
    public static String getFileUrl(String bucketName, String filename, Date expire){
        URL url = amazonS3.generatePresignedUrl(bucketName, filename, expire);
        return url.toString();
    }
}
