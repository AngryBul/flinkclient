package com.asiainfo.connect.sink;

import com.asiainfo.connect.jobconfig.ConstantConfig;
import com.asiainfo.connect.jobconfig.JobConfigBean;
import com.asiainfo.connect.util.CommonUtils;
import com.asiainfo.connect.util.PhoenixConnectionFactory;
import com.asiainfo.connect.util.PhoenixConnectionPool;
import com.asiainfo.messageparse.impl.OggMessageParse;
import com.asiainfo.messageparse.impl.TradelogTableParse;
import com.asiainfo.messageparse.inf.ITableMessageParse;
import com.asiainfo.oggmessage.OggMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class PhoenixSink extends RichSinkFunction {

    private Logger logger;

    private Connection connection;

    private String jobId;

    private JobConfigBean jobConfig;

    private Map<String,LinkedBlockingQueue<ConsumerRecord<String,String>>> messageQueue;

    private Map<String,ITableMessageParse> implTableParseMap;

    public PhoenixSink(String jobId,JobConfigBean jobConfig)
    {
        this.jobId = jobId;
        this.jobConfig = jobConfig;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger = LoggerFactory.getLogger(PhoenixSink.class);
        PhoenixConnectionPool connectionPool = PhoenixConnectionFactory.createConnnectionPool(ConstantConfig.ZK_URL);
        messageQueue= new ConcurrentHashMap<>();
        implTableParseMap = new ConcurrentHashMap<>();
        String tables = jobConfig.getTableName();
        String[] tableArray = tables.split(",",-1);
        System.out.println("tableArray="+Arrays.toString(tableArray));
        connection = connectionPool.getConnection();
        for (String table:tableArray)
        {
            String[] tableParses = table.split(":",-1);
            System.out.println("tableParses[0]="+tableParses[0]);
            messageQueue.put(tableParses[0],new LinkedBlockingQueue<>());
            implTableParseMap.put(tableParses[0],(ITableMessageParse) Class.forName(tableParses[1]).getDeclaredConstructors()[0].newInstance());

            new Thread(()->{
                Map partitionOffsetMap = new ConcurrentHashMap<>();
                Statement statement = null;
                Connection connection = null;
                try {
                    connection = connectionPool.getConnection();
                    connection.setAutoCommit(false);
                    statement = connection.createStatement();
                } catch (SQLException e) {
                    System.out.println(e);
                    e.printStackTrace();
                }
                LinkedBlockingQueue<ConsumerRecord<String,String>> tableQueue =  messageQueue.get(tableParses[0]);
                long count = 0L;
                long start = System.currentTimeMillis();
                while(true)
                {
                    ConsumerRecord<String,String> consumerRecord = tableQueue.poll();
                    if(consumerRecord!=null)
                    {
                        try {
                            String sql = genSql(consumerRecord.value(),tableParses[0]);
                            if(sql!=null)
                            {
                                statement.addBatch(sql);
                            }
                        } catch (Exception e) {
                            System.out.println(e);
                            logger.error("入表 "+tableParses[0].toUpperCase()+" 出错!",e);
                        }

                        partitionOffsetMap.put("topic",consumerRecord.topic());
                        partitionOffsetMap.put("groupid",jobConfig.getGroupId());
                        partitionOffsetMap.put("tablename",tableParses[0].toUpperCase());
                        partitionOffsetMap.put("partition",String.valueOf(consumerRecord.partition()));
                        partitionOffsetMap.put("partitionoffset",String.valueOf(consumerRecord.offset()));
                        partitionOffsetMap.put("timestamp",CommonUtils.getCurrentTime());
                        count++;
                    }
                    long now = System.currentTimeMillis();
                    if((count!=0&&count%3000==0)||(now - start>10000&&!partitionOffsetMap.isEmpty()))
                    {
                        start = now;
                        count = 0;
                        String offsetSql = genOffsetSql(partitionOffsetMap);
                        System.out.println(Thread.currentThread().getName()+" : offsetsql="+offsetSql);
                        try {
                            statement.addBatch(offsetSql);
                            statement.executeBatch();
                            connection.commit();
                            statement.close();
                            statement = connection.createStatement();
                        } catch (SQLException e) {
                            e.printStackTrace();
                            System.out.println(e);
                            logger.error("入表 kafka_offset 出错!",e);
                        }

                    }

                }
            }).start();
        }

    }

    public void invoke(Object value, Context context) throws Exception {

        if(value instanceof ConsumerRecords)
        {
            ConsumerRecords<String,String> consumerRecords = (ConsumerRecords) value;
            Statement statement =  connection.createStatement();
            for (ConsumerRecord<String,String> consumerRecord : consumerRecords)
            {
                String message = consumerRecord.value();
                Set<String> tables = messageQueue.keySet();
                for(String tableName:tables)
                {
                    if(message!=null&&message.indexOf(tableName.toUpperCase())!=-1)
                    {
                        messageQueue.get(tableName).add(consumerRecord);
                    }
                }

            }
        }
        else
        {

        }

    }

    private String genSql(String message ,String confTableName) throws Exception {
        String sql = null;
        byte[] bytes = message.getBytes();
        OggMessage oggMessage = OggMessageParse.parse(bytes);
        ITableMessageParse tableMessageParse = implTableParseMap.get(confTableName);
        Map<String, Map<String, String>> tableMap = tableMessageParse.tableMessageParse(oggMessage);
        Map<String,String> currentValueMap = tableMap.get(ITableMessageParse.CURRENT_COLUMN_MAP);
        Map<String,String> oldValueMap = tableMap.get(ITableMessageParse.OLD_COLUMN_MAP);
        Map<String,String> headMap = tableMap.get(ITableMessageParse.HEAD);
        String operate = headMap.get(ITableMessageParse.OPERATE);
        String tableName = headMap.get(ITableMessageParse.TABLE_NAME);
        if(!confTableName.toUpperCase().equals(tableName))
        {
            return sql;
        }
        if("Insert".equals(operate)||"Update".equals(operate))
        {
            sql = "upsert into %s (%s) values (%s)";
            StringBuffer sb = new StringBuffer();
            StringBuffer sb1 = new StringBuffer();
            Set<Map.Entry<String, String>> currentEntry = currentValueMap.entrySet();
            for(Map.Entry<String, String> entry : currentEntry)
            {
                String entryKey = entry.getKey();
                String entryValue =entry.getValue();
                sb.append(entryKey).append(",");
                if(entryValue==null)
                {
                    sb1.append(entryValue).append(",");
                }
                else if (entryValue!=null && ! tableMessageParse.checkDate(entryKey))
                {
                    sb1.append("'").append(entryValue).append("'").append(",");
                }
                else if(entryValue!=null && tableMessageParse.checkDate(entryKey))
                {
                    sb1.append("TO_DATE('").append(entryValue).append("','yyyy-MM-dd:HH:mm:ss')").append(",");
                }

            }
            sql = String.format(sql,confTableName,sb.toString().substring(0,sb.toString().length()-1),sb1.toString().substring(0,sb1.toString().length()-1));

        }
        else if("Delete".equals(operate))
        {
            sql = "delete from %s where (%s)";
            StringBuffer sb = new StringBuffer();
            Set<Map.Entry<String, String>> oldEntry = oldValueMap.entrySet();
            for(Map.Entry<String, String> entry : oldEntry)
            {
                String entryKey = entry.getKey();
                String entryValue =entry.getValue();
                if(entryValue==null)
                {
                    sb.append(entryKey).append("=").append(entryValue).append(" and ");
                }
                else if (entryValue!=null && !tableMessageParse.checkDate(entryKey))
                {
                    sb.append(entryKey).append("=").append("'").append(entryValue).append("'").append(" and ");
                }
                else if(entryValue!=null && tableMessageParse.checkDate(entryKey))
                {
                    sb.append(entryKey).append("=").append("TO_DATE('").append(entryValue).append("','yyyy-MM-dd:hh:mm:ss')").append(" and ");
                }

            }
            sql = String.format(sql,confTableName,sb.toString().substring(0,sb.toString().length()-4));
        }

        return sql;

    }

    private String genOffsetSql(Map<String ,String> map)
    {
        String sql = "upsert into kafka_offset (%s) values (%s)";
        Set<Map.Entry<String, String>> entries = map.entrySet();
        StringBuffer keysb = new StringBuffer();
        StringBuffer valuesb = new StringBuffer();
        for(Map.Entry<String, String> entry:entries)
        {
            String entryKey = entry.getKey();
            String entryValue =entry.getValue();
            keysb.append(entryKey).append(",");
            if(entryValue==null)
            {
                valuesb.append(entryValue).append(",");
            }
            else if (entryValue!=null && !"timestamp".equals(entryKey))
            {
                valuesb.append("'").append(entryValue).append("'").append(",");
            }
            else if(entryValue!=null && "timestamp".equals(entryKey))
            {
                valuesb.append("TO_DATE('").append(entryValue).append("','yyyy-MM-dd hh:mm:ss')").append(",");
            }

        }
        sql = String.format(sql,keysb.toString().substring(0,keysb.toString().length()-1),valuesb.toString().substring(0,valuesb.toString().length()-1));


        return sql;
    }
}
