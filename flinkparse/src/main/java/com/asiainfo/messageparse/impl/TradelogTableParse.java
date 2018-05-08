package com.asiainfo.messageparse.impl;

import com.asiainfo.oggmessage.Column;
import com.asiainfo.oggmessage.OggMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TradelogTableParse extends OggMessageParse {
    //index:0
    public static final String INTF_ID = "INTF_ID";
    //1
    public static final String TRADE_ID = "TRADE_ID";
    //2
    public static final String CHANNEL_ID = "CHANNEL_ID";
    //3
    public static final String ACCEPT_EPARCHY = "ACCEPT_EPARCHY";
    //4
    public static final String CITY_CODE = "CITY_CODE";
    //5
    public static final String DEPART_CODE = "DEPART_CODE";
    //6
    public static final String STAFF_ID = "STAFF_ID";
    //7
    public static final String EPARCHY_CODE = "EPARCHY_CODE";
    //8
    public static final String ACCEPT_TIME = "ACCEPT_TIME";
    //9
    public static final String CHANNEL_TRADE_ID = "CHANNEL_TRADE_ID";
    //10
    public static final String CHANNEL_ACCEPT_TIME = "CHANNEL_ACCEPT_TIME";
    //11
    public static final String USER_ID = "USER_ID";
    //12
    public static final String FEE = "FEE";
    //13
    public static final String CANCEL_TAG = "CANCEL_TAG";
    //14
    public static final String CANCEL_EPARCHY = "CANCEL_EPARCHY";
    //15
    public static final String CANCEL_CITY_CODE = "CANCEL_CITY_CODE";
    //16
    public static final String CANCEL_DEPART_CODE = "CANCEL_DEPART_CODE";
    //17
    public static final String CANCEL_STAFF_ID = "CANCEL_STAFF_ID";
    //18
    public static final String CANCEL_TIME = "CANCEL_TIME";
    //19
    public static final String CHANNEL_CANCEL_TIME = "CHANNEL_CANCEL_TIME";
    //20
    public static final String CHARGE_SOURCE_CODE = "CHARGE_SOURCE_CODE";
    //21
    public static final String PAY_FEE_MODE_CODE = "PAY_FEE_MODE_CODE";
    //22
    public static final String EFFECT_TAG = "EFFECT_TAG";
    //23
    public static final String CARRIER_CODE = "CARRIER_CODE";
    //24
    public static final String CARRIER_ID = "CARRIER_ID";
    //25
    public static final String PREVALUEC1 = "PREVALUEC1";
    //26
    public static final String PREVALUEC2 = "PREVALUEC2";
    //27
    public static final String PREVALUEC3 = "PREVALUEC3";
    //28
    public static final String PREVALUE1 = "PREVALUE1";
    //29
    public static final String PREVALUE2 = "PREVALUE2";
    //30
    public static final String PREVALUE3 = "PREVALUE3";
    //31
    public static final String PREVALUE4 = "PREVALUE4";
    //32
    public static final String PREVALUE5 = "PREVALUE5";
    //33
    public static final String PREVALUE6 = "PREVALUE6";
    //34
    public static final String PREVALUE7 = "PREVALUE7";
    //35
    public static final String PREVALUE8 = "PREVALUE8";
    //36
    public static final String PREVALUE9 = "PREVALUE9";
    //37
    public static final String PREVALUE10 = "PREVALUE10";
    //38
    public static final String PREVALUEN1 = "PREVALUEN1";
    //39
    public static final String PREVALUEN2 = "PREVALUEN2";
    //40
    public static final String PREVALUEN3 = "PREVALUEN3";
    //41
    public static final String PREVALUED1 = "PREVALUED1";
    //42
    public static final String PREVALUED2 = "PREVALUED2";
    //43
    public static final String PREVALUED3 = "PREVALUED3";
    //44
    public static final String MONTH = "MONTH";
    //45
    public static final String PROV_CODE = "PROV_CODE";
    //46
    public static final String CARD_TYPE_CODE = "CARD_TYPE_CODE";
    //47
    public static final String PRINT_FLAG = "PRINT_FLAG";
    //48
    public static final String PRINT_FEE = "PRINT_FEE";
    //49
    public static final String FPAY_FEE = "FPAY_FEE";
    //50
    public static final String CUTOFFDAY = "CUTOFFDAY";


    public  Map<String, Map<String, String>> tableMessageParse(OggMessage oggMessage) {
        Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>();
        Map<String, String> headMap = new HashMap<String, String>();
        Map<String, String> currentValueMap = new HashMap<String, String>();
        Map<String, String> oldValueMap = new HashMap<String, String>();
        List<Column> columns = oggMessage.getColumns();
        headMap.put(UU_ID, new String(oggMessage.getUuid()));
        headMap.put(SCN, oggMessage.getScn() + "");
        headMap.put(OGG_TID, new String(oggMessage.getOggTransactionId()));
        headMap.put(LOCAL_TID, new String(oggMessage.getLocalTransactionId()));
        headMap.put(SCHEME, new String(oggMessage.getSchemeName()));
        headMap.put(TABLE_NAME, new String(oggMessage.getTableName()));
        headMap.put(OPERATE, oggMessage.getOperate().name());
        headMap.put(TIME_STAMP, oggMessage.getTimestampInMicroSeconds() + "");
        //Calendar时间
        headMap.put(DATE, oggMessage.getTimestamp() + "");

        for (Column column : columns) {
            switch (column.getIndex()) {
                case 0:
                    putCurrentValue(column, INTF_ID, currentValueMap);
                    putOldValue(column, INTF_ID, oldValueMap);
                    break;
                case 1:
                    putCurrentValue(column, TRADE_ID, currentValueMap);
                    putOldValue(column, TRADE_ID, oldValueMap);
                    break;
                case 2:
                    putCurrentValue(column, CHANNEL_ID, currentValueMap);
                    putOldValue(column, CHANNEL_ID, oldValueMap);
                    break;
                case 3:
                    putCurrentValue(column, ACCEPT_EPARCHY, currentValueMap);
                    putOldValue(column, ACCEPT_EPARCHY, oldValueMap);
                    break;
                case 4:
                    putCurrentValue(column, CITY_CODE, currentValueMap);
                    putOldValue(column, CITY_CODE, oldValueMap);
                    break;
                case 5:
                    putCurrentValue(column, DEPART_CODE, currentValueMap);
                    putOldValue(column, DEPART_CODE, oldValueMap);
                    break;
                case 6:
                    putCurrentValue(column, STAFF_ID, currentValueMap);
                    putOldValue(column, STAFF_ID, oldValueMap);
                    break;
                case 7:
                    putCurrentValue(column, EPARCHY_CODE, currentValueMap);
                    putOldValue(column, EPARCHY_CODE, oldValueMap);
                    break;
                case 8:
                    putCurrentValue(column, ACCEPT_TIME, currentValueMap);
                    putOldValue(column, ACCEPT_TIME, oldValueMap);
                    break;
                case 9:
                    putCurrentValue(column, CHANNEL_TRADE_ID, currentValueMap);
                    putOldValue(column, CHANNEL_TRADE_ID, oldValueMap);
                    break;
                case 10:
                    putCurrentValue(column, CHANNEL_ACCEPT_TIME, currentValueMap);
                    putOldValue(column, CHANNEL_ACCEPT_TIME, oldValueMap);
                    break;
                case 11:
                    putCurrentValue(column, USER_ID, currentValueMap);
                    putOldValue(column, USER_ID, oldValueMap);
                    break;
                case 12:
                    putCurrentValue(column, FEE, currentValueMap);
                    putOldValue(column, FEE, oldValueMap);
                    break;
                case 13:
                    putCurrentValue(column, CANCEL_TAG, currentValueMap);
                    putOldValue(column, CANCEL_TAG, oldValueMap);
                    break;
                case 14:
                    putCurrentValue(column, CANCEL_EPARCHY, currentValueMap);
                    putOldValue(column, CANCEL_EPARCHY, oldValueMap);
                    break;
                case 15:
                    putCurrentValue(column, CANCEL_CITY_CODE, currentValueMap);
                    putOldValue(column, CANCEL_CITY_CODE, oldValueMap);
                    break;
                case 16:
                    putCurrentValue(column, CANCEL_DEPART_CODE, currentValueMap);
                    putOldValue(column, CANCEL_DEPART_CODE, oldValueMap);
                    break;
                case 17:
                    putCurrentValue(column, CANCEL_STAFF_ID, currentValueMap);
                    putOldValue(column, CANCEL_STAFF_ID, oldValueMap);
                    break;
                case 18:
                    putCurrentValue(column, CANCEL_TIME, currentValueMap);
                    putOldValue(column, CANCEL_TIME, oldValueMap);
                    break;
                case 19:
                    putCurrentValue(column, CHANNEL_CANCEL_TIME, currentValueMap);
                    putOldValue(column, CHANNEL_CANCEL_TIME, oldValueMap);
                    break;
                case 20:
                    putCurrentValue(column, CHARGE_SOURCE_CODE, currentValueMap);
                    putOldValue(column, CHARGE_SOURCE_CODE, oldValueMap);
                    break;
                case 21:
                    putCurrentValue(column, PAY_FEE_MODE_CODE, currentValueMap);
                    putOldValue(column, PAY_FEE_MODE_CODE, oldValueMap);
                    break;
                case 22:
                    putCurrentValue(column, EFFECT_TAG, currentValueMap);
                    putOldValue(column, EFFECT_TAG, oldValueMap);
                    break;
                case 23:
                    putCurrentValue(column, CARRIER_CODE, currentValueMap);
                    putOldValue(column, CARRIER_CODE, oldValueMap);
                    break;
                case 24:
                    putCurrentValue(column, CARRIER_ID, currentValueMap);
                    putOldValue(column, CARRIER_ID, oldValueMap);
                    break;
                case 25:
                    putCurrentValue(column, PREVALUEC1, currentValueMap);
                    putOldValue(column, PREVALUEC1, oldValueMap);
                    break;
                case 26:
                    putCurrentValue(column, PREVALUEC2, currentValueMap);
                    putOldValue(column, PREVALUEC2, oldValueMap);
                    break;
                case 27:
                    putCurrentValue(column, PREVALUEC3, currentValueMap);
                    putOldValue(column, PREVALUEC3, oldValueMap);
                    break;
                case 28:
                    putCurrentValue(column, PREVALUE1, currentValueMap);
                    putOldValue(column, PREVALUE1, oldValueMap);
                    break;
                case 29:
                    putCurrentValue(column, PREVALUE2, currentValueMap);
                    putOldValue(column, PREVALUE2, oldValueMap);
                    break;
                case 30:
                    putCurrentValue(column, PREVALUE3, currentValueMap);
                    putOldValue(column, PREVALUE3, oldValueMap);
                    break;
                case 31:
                    putCurrentValue(column, PREVALUE4, currentValueMap);
                    putOldValue(column, PREVALUE4, oldValueMap);
                    break;
                case 32:
                    putCurrentValue(column, PREVALUE5, currentValueMap);
                    putOldValue(column, PREVALUE5, oldValueMap);
                    break;
                case 33:
                    putCurrentValue(column, PREVALUE6, currentValueMap);
                    putOldValue(column, PREVALUE6, oldValueMap);
                    break;
                case 34:
                    putCurrentValue(column, PREVALUE7, currentValueMap);
                    putOldValue(column, PREVALUE7, oldValueMap);
                    break;
                case 35:
                    putCurrentValue(column, PREVALUE8, currentValueMap);
                    putOldValue(column, PREVALUE8, oldValueMap);
                    break;
                case 36:
                    putCurrentValue(column, PREVALUE9, currentValueMap);
                    putOldValue(column, PREVALUE9, oldValueMap);
                    break;
                case 37:
                    putCurrentValue(column, PREVALUE10, currentValueMap);
                    putOldValue(column, PREVALUE10, oldValueMap);
                    break;
                case 38:
                    putCurrentValue(column, PREVALUEN1, currentValueMap);
                    putOldValue(column, PREVALUEN1, oldValueMap);
                    break;
                case 39:
                    putCurrentValue(column, PREVALUEN2, currentValueMap);
                    putOldValue(column, PREVALUEN2, oldValueMap);
                    break;
                case 40:
                    putCurrentValue(column, PREVALUEN3, currentValueMap);
                    putOldValue(column, PREVALUEN3, oldValueMap);
                    break;
                case 41:
                    putCurrentValue(column, PREVALUED1, currentValueMap);
                    putOldValue(column, PREVALUED1, oldValueMap);
                    break;
                case 42:
                    putCurrentValue(column, PREVALUED2, currentValueMap);
                    putOldValue(column, PREVALUED2, oldValueMap);
                    break;
                case 43:
                    putCurrentValue(column, PREVALUED3, currentValueMap);
                    putOldValue(column, PREVALUED3, oldValueMap);
                    break;
                case 44:
                    putCurrentValue(column, MONTH, currentValueMap);
                    putOldValue(column, MONTH, oldValueMap);
                    break;
                case 45:
                    putCurrentValue(column, PROV_CODE, currentValueMap);
                    putOldValue(column, PROV_CODE, oldValueMap);
                    break;
                case 46:
                    putCurrentValue(column, CARD_TYPE_CODE, currentValueMap);
                    putOldValue(column, CARD_TYPE_CODE, oldValueMap);
                    break;
                case 47:
                    putCurrentValue(column, PRINT_FLAG, currentValueMap);
                    putOldValue(column, PRINT_FLAG, oldValueMap);
                    break;
                case 48:
                    putCurrentValue(column, PRINT_FEE, currentValueMap);
                    putOldValue(column, PRINT_FEE, oldValueMap);
                    break;
                case 49:
                    putCurrentValue(column, FPAY_FEE, currentValueMap);
                    putOldValue(column, FPAY_FEE, oldValueMap);
                    break;
                case 50:
                    putCurrentValue(column, CUTOFFDAY, currentValueMap);
                    putOldValue(column, CUTOFFDAY, oldValueMap);
                    break;
            }
        }
        resultMap.put(HEAD, headMap);
        resultMap.put(CURRENT_COLUMN_MAP, currentValueMap);
        resultMap.put(OLD_COLUMN_MAP, oldValueMap);

        return resultMap;
    }

    private void putCurrentValue(Column c, String key, Map<String, String> map) {
        if (c.isCurrentValueExist()) {
            map.put(key, c.getCurrentValue() == null ? null : new String(c.getCurrentValue()));
        }
    }

    private void putOldValue(Column c, String key, Map<String, String> map) {
        if (c.isOldValueExist()) {
            map.put(key, c.getCurrentValue() == null ? null : new String(c.getOldValue()));
        }
    }

    @Override
    public boolean checkDate(String str) {
        if("ACCEPT_TIME".equals(str)||"CHANNEL_ACCEPT_TIME".equals(str)||"CANCEL_TIME".equals(str)||"CHANNEL_CANCEL_TIME".equals(str)||"PREVALUED1".equals(str)||"PREVALUED2".equals(str)||"PREVALUED3".equals(str))
        {
            return true;
        }else
        {
            return false;
        }
    }
}
