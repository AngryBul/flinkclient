package com.asiainfo.connect.jobconfig;

import java.io.Serializable;

public class JobConfigBean implements Serializable {
    private String sourceName;
    private String sinkName;
    private String tableName;
    private String groupId;
    private String topicName;
    private String consumePartition;
    private String parallelism;


    public String getParallelism() {
        return parallelism;
    }

    public void setParallelism(String parallelism) {
        this.parallelism = parallelism;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getSinkName() {
        return sinkName;
    }

    public void setSinkName(String sinkName) {
        this.sinkName = sinkName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getConsumePartition() {
        return consumePartition;
    }

    public void setConsumePartition(String consumePartition) {
        this.consumePartition = consumePartition;
    }
}
