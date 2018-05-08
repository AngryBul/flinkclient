package com.asiainfo.connect.consumer;

import com.asiainfo.connect.jobconfig.ConstantConfig;
import com.asiainfo.connect.jobconfig.JobConfigBean;
import com.asiainfo.connect.util.PhoenixConnectionFactory;
import com.asiainfo.connect.util.PhoenixConnectionPool;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class SimpleKafkaConsumer extends RichParallelSourceFunction {

    private boolean isRunning = true;

    private String jobId;

    private JobConfigBean jobConfig;

    private KafkaConsumer consumer;

    public SimpleKafkaConsumer(String jobId ,JobConfigBean jobConfig)
    {
        this.jobId=jobId;
        this.jobConfig =jobConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        PhoenixConnectionPool connectionPool = PhoenixConnectionFactory.createConnnectionPool(ConstantConfig.ZK_URL);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", ConstantConfig.BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", jobConfig.getGroupId());
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("zookeeper.connect",ConstantConfig.ZOOKEEPER_CONNECT);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ArrayList<Integer> arrayList = new ArrayList<>();
        consumer = new KafkaConsumer(properties);
        RuntimeContext runtimeContext =this.getRuntimeContext();
        int thisSubtask = runtimeContext.getIndexOfThisSubtask();
        int parallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();

        ArrayList<Integer> partitionList = null;
        String consumePartition = jobConfig.getConsumePartition();
        if(consumePartition!=null&&!"".equals(consumePartition))
        {
            String[] partitions = consumePartition.split(",",-1);
            partitionList = new ArrayList<>();
            for(String partition : partitions)
            {
                partitionList.add(Integer.parseInt(partition));
            }
        }

        if(partitionList != null)
        {
            if(parallelSubtasks > partitionList.size())
            {
                parallelSubtasks = partitionList.size();
            }
            if(thisSubtask+1 > parallelSubtasks)
            {
                this.close();
                return;
            }
            arrayList = getDefaultSubPartitions(thisSubtask , parallelSubtasks,partitionList);
        }

        ArrayList<TopicPartition> topicPartitions= new ArrayList<>();

        for(Integer i:arrayList)
        {
            TopicPartition topicPartition = new TopicPartition(jobConfig.getTopicName(),i);
            topicPartitions.add(topicPartition);
        }

        consumer.assign(topicPartitions);

        Set<TopicPartition> partitions = consumer.assignment();

        Map<Integer, Long> consumOffset = getOffset(connectionPool, jobConfig.getGroupId(), jobConfig.getTopicName());

        System.out.println("consumOffset="+consumOffset);


        for(TopicPartition partition : partitions)
        {
            Long offset = consumOffset.get(partition.partition());
            if(offset==null)
            {
                offset=0L;
            }
            consumer.seek(new TopicPartition(jobConfig.getTopicName(),partition.partition()),offset);
        }
        
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.cancel();
    }

    public void run(SourceContext sourceContext) throws Exception {
        while(isRunning)
        {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(10000);
                if(!records.isEmpty())
                {
                    sourceContext.collect(records);
                }

            }
        }

    }

    public void cancel() {
        this.isRunning = false;

    }

    private static Map<Integer,Long> getOffset(PhoenixConnectionPool connectionPool,String groupid,String topic)
    {
        Map<Integer,Long> resultMap = new HashMap<>();
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "select partitionoffset,partition from kafka_offset where groupid='%s' and topic='%s'";
            sql = String.format(sql,groupid,topic);
            Statement statement = connection.createStatement();
            System.out.println("sql="+sql);
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next())
            {
                Long offset = Long.parseLong(resultSet.getString("partitionoffset"));
                Integer partition = Integer.parseInt(resultSet.getString("partition"));
                resultMap.put(partition,offset);
            }
            resultSet.close();
            statement.close();
            connection.close();
            return resultMap;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }


    private ArrayList<Integer> getDefaultSubPartitions(int thisSubtask , int parallelSubtasks , ArrayList<Integer> defaultPartitionList)
    {
        ArrayList<Integer> subPartition = new ArrayList<>();
        if(parallelSubtasks == defaultPartitionList.size() )
        {
            subPartition.add(defaultPartitionList.get(thisSubtask));
        }
        else
        {
            int i = defaultPartitionList.size()/parallelSubtasks;
            int j = defaultPartitionList.size()%parallelSubtasks;
            for(int l=1 ;l<parallelSubtasks+1;l++)
            {
                if(thisSubtask+1==l)
                {
                    for(int m=(l-1)*i;m<l*i;m++)
                    {
                        subPartition.add(defaultPartitionList.get(m));
                    }
                    for(int n=0;n<j;n++)
                    {
                        if(thisSubtask==n)
                        {
                            subPartition.add(defaultPartitionList.get(i*parallelSubtasks+n));
                        }
                    }
                }

            }
        }

        return subPartition;

    }
}
