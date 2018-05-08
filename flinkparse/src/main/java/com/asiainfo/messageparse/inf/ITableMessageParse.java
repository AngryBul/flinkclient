package com.asiainfo.messageparse.inf;

import com.asiainfo.oggmessage.OggMessage;

import java.io.Serializable;
import java.util.Map;

public interface ITableMessageParse extends Serializable {

    //ogg公共字段
    public static final String HEAD = "head";
    //currentValueMap
    public static final String CURRENT_COLUMN_MAP = "currentColumnMap";
    //oldValueMap
    public static final String OLD_COLUMN_MAP = "oldColumnMap";
    //ogg:uuid
    public static final String UU_ID = "uuid";
    //scn
    public static final String SCN = "scn";
    //oggTid
    public static final String OGG_TID = "oggTid";
    //localTid
    public static final String LOCAL_TID = "localTid";
    //scheme
    public static final String SCHEME = "scheme";
    //tablename
    public static final String TABLE_NAME = "tablename";
    //operate
    public static final String OPERATE = "operate";
    //Timestamp
    public static final String TIME_STAMP = "Timestamp";
    //date
    public static final String DATE = "date";

    public abstract Map<String,Map<String,String>> tableMessageParse(OggMessage oggMessage);

    public boolean checkDate(String str);
}
