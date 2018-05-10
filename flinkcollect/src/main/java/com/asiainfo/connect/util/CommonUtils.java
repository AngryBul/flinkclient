package com.asiainfo.connect.util;

import com.asiainfo.connect.jobconfig.ConstantConfig;
import com.asiainfo.connect.jobconfig.JobConfigBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonUtils {

    private static FileSystem fs;

    private static  final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    static
    {
        Configuration conf = new Configuration();
        try {
            fs= FileSystem.get(new URI(ConstantConfig.HDFS_URI), conf, "root");
        } catch (Exception e) {
            logger.error("HDFS 连接出错",e);
            e.printStackTrace();

        }

    }

    private CommonUtils()
    {
    }

    public static final JobConfigBean getJobConfig(String jobId)
    {
        PhoenixConnectionPool connectionPool = PhoenixConnectionFactory.createConnnectionPool(ConstantConfig.ZK_URL);
        JobConfigBean jobConfigBean = new JobConfigBean();
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "select sourcename,sinkname,tablename,groupid,topicname,consumepartitions,parallelism from flink_job_config where jobid=%s";
            sql = String.format(sql,jobId);
            Statement statement = connection.createStatement();
            System.out.println("sql="+sql);
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next())
            {
                jobConfigBean.setSourceName(resultSet.getString("sourcename"));
                jobConfigBean.setSinkName(resultSet.getString("sinkname"));
                jobConfigBean.setTableName(resultSet.getString("tablename"));
                jobConfigBean.setGroupId(resultSet.getString("groupid"));
                jobConfigBean.setTopicName(resultSet.getString("topicname"));
                jobConfigBean.setConsumePartition(resultSet.getString("consumepartitions"));
                jobConfigBean.setParallelism(resultSet.getString("parallelism"));
            }
            resultSet.close();
            statement.close();
            connection.close();
            return jobConfigBean;

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static final String getCurrentTime()
    {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return simpleDateFormat.format(date);
    }

    public static final boolean put2Hdfs(String src,String dest)
    {

        try {
            fs.copyFromLocalFile(new Path(src),new Path(dest));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

}
