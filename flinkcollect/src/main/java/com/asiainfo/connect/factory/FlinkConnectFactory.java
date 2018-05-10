package com.asiainfo.connect.factory;

import com.asiainfo.connect.jobconfig.JobConfigBean;
import com.asiainfo.connect.util.CommonUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkConnectFactory {


    public static class  StreamConnector
    {
        public  void createJob(String jobId) throws Exception {
            JobConfigBean jobConfig = CommonUtils.getJobConfig(jobId);
            String parallelism = jobConfig.getParallelism();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(Integer.parseInt(parallelism));
            env.enableCheckpointing(1000);
            SourceFunction<Object> sourceFunction = (SourceFunction<Object>)Class.forName(jobConfig.getSourceName()).getDeclaredConstructor(String.class,JobConfigBean.class).newInstance(jobId,jobConfig);
            SinkFunction sinkFunction = (SinkFunction) Class.forName(jobConfig.getSinkName()).getDeclaredConstructor(String.class,JobConfigBean.class).newInstance(jobId,jobConfig);
            DataStream stream =env.addSource(sourceFunction);
            stream.addSink(sinkFunction);
            env.execute();
        }
    }

    public static class  DataSetConnector
    {
        public static void createJob(String jobId) throws Exception {
            ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        }
    }


}
