package com.sitech.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {
    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
//        configuration.set("fs.default.name", "hdfs://hadoop102:9000");
//        FileSystem fs = FileSystem.get(configuration);
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"hadoop");
    }

    @Test
    public void put() throws IOException {
        fs.copyFromLocalFile(new Path("/Users/ikki/Desktop/wordcounttest.txt"),new Path("/"));

    }

    @Test
    public void get() throws IOException {
        fs.copyToLocalFile(new Path("/test2"),new Path("/anaconda3/test1.txt"));

    }

    @Test
    public void create() throws IOException {
        fs.create(new Path("/test2"));

    }

    @Test
    public void rename() throws IOException {
        fs.rename(new Path("/test2"),new Path("/test3"));

    }

    @Test
    public void delete() throws IOException {
        fs.delete(new Path("/test1.txt"),true);

    }

    @Test
    public void testListFile() throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------班长的分割线----------");
        }
    }

    @Test
    public void testLS() throws IOException {

        FileStatus[] fileState = fs.listStatus(new Path("/"));

        for(FileStatus fileStatus:fileState){
            if(fileStatus.isFile()){
                System.out.println("这是文件信息");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
            }
            else {
                System.out.println("这是一个文件夹");
                System.out.println(fileStatus.getPath());
            }

        }
    }

    @After
    public void after() throws IOException {
        fs.close();
        System.out.println("over");
    }


}
