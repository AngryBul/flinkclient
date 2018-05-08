package com.asiainfo.oggmessage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class OggMessage implements Serializable{


    /**
     * 唯一标示
     */
    private byte[] uuid;

    /**
     * scn
     */
    private long scn;

    /**
     * OGG 事务ID
     */
    private byte[] oggTransactionId;

    /**
     * 本地事务ID
     */
    private byte[] localTransactionId;

    /**
     * 该操作在事务中的状态/位置
     */
    private OperateState opState;

    /**
     *
     */
    private byte[] catalogName;

    /**
     * 模式名
     */
    private byte[] schemeName;

    /**
     * 表名
     */
    private byte[] tableName;

    /**
     * 操作
     */
    private Operate operate;

    /**
     * 操作发生的时间： 微妙表示, 注意Java的为毫秒
     */
    private long timestampInMicroSeconds;

    /**
     * 列
     */
    private List<Column> columns;


    /**
     * 列
     */
    private HashMap<Integer,Column> columnMap;

    /**
     * key
     */
    private String key;

    /**
     * key
     */
    private String oldKey;

    /**
     * 时间戳
     */
    private String strDataStamp;

    private String strTableName;


    public byte[] getUuid() {
        return uuid;
    }

    public void setUuid(byte[] uuid) {
        this.uuid = uuid;
    }

    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public byte[] getOggTransactionId() {
        return oggTransactionId;
    }

    public void setOggTransactionId(byte[] oggTransactionId) {
        this.oggTransactionId = oggTransactionId;
    }

    public byte[] getLocalTransactionId() {
        return localTransactionId;
    }

    public void setLocalTransactionId(byte[] localTransactionId) {
        this.localTransactionId = localTransactionId;
    }

    public OperateState getOpState() {
        return opState;
    }

    public void setOpState(OperateState opState) {
        this.opState = opState;
    }

    public byte[] getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(byte[] catalogName) {
        this.catalogName = catalogName;
    }

    public byte[] getSchemeName() {
        return schemeName;
    }

    public void setSchemeName(byte[] schemeName) {
        this.schemeName = schemeName;
    }

    public byte[] getTableName() {
        return tableName;
    }

    public void setTableName(byte[] tableName) {
        this.tableName = tableName;
    }

    public Operate getOperate() {
        return operate;
    }

    public void setOperate(Operate operate) {
        this.operate = operate;
    }

    public long getTimestampInMicroSeconds() {
        return timestampInMicroSeconds;
    }

    public long getTimestamp() {
        return (timestampInMicroSeconds + 500) / 1000;
    }

    public void setTimestampInMicroSeconds(long timestamp) {
        this.timestampInMicroSeconds = timestamp;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public String getOldKey() {
        return oldKey;
    }

    public void setOldKey(String oldKey) {
        this.oldKey = oldKey;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getStrDataStamp() {
        return strDataStamp;
    }

    public void setStrDataStamp(String strDataStamp) {
        this.strDataStamp = strDataStamp;
    }

    public String getStrTableName() {
        return strTableName;
    }

    public void setStrTableName(String strTableName) {
        this.strTableName = strTableName;
    }

    public HashMap<Integer, Column> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(HashMap<Integer, Column> columnMap) {
        this.columnMap = columnMap;
    }
}
