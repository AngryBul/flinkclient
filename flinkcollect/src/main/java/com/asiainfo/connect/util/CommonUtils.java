package com.asiainfo.connect.util;

import com.asiainfo.connect.jobconfig.ConstantConfig;
import com.asiainfo.connect.jobconfig.JobConfigBean;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonUtils {
    private CommonUtils()
    {
    }

    public static final JobConfigBean getJobConfig(String jobId)
    {
        PhoenixConnectionPool connectionPool = PhoenixConnectionFactory.createConnnectionPool(ConstantConfig.ZK_URL);
        JobConfigBean jobConfigBean = new JobConfigBean();
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "select sourcename,sinkname,tablename,groupid,topicname,consumepartition,parallelism from flink_job_config where jobid=%s";
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
                jobConfigBean.setConsumePartition(resultSet.getString("consumepartition"));
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

}
