package com.asiainfo.messageparse.test;

import com.asiainfo.messageparse.impl.OggMessageParse;
import com.asiainfo.messageparse.impl.TradelogTableParse;
import com.asiainfo.messageparse.inf.ITableMessageParse;
import com.asiainfo.oggmessage.Column;
import com.asiainfo.oggmessage.OggMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class MessageparseTest {
    private String oggmessage;


    @Before
    public void atBefore()
    {
        oggmessage = "ACT_CUE1\u000215159901219867\u000200004769070079076186\u00020\u00022\u0001UCR_ACT1.TL_B_TRADELOG\u0001I\u00012018-04-10 04:19:30.007500\u00010\u00022018041093609597\u0004\u00011\u00028618041015605054\u0004\u00012\u0002Z000YKC\u0004\u00013\u00020871\u0004\u00014\u00020871\u0004\u00015\u0002Z00DZQD\u0004\u00016\u0002Z000DZQD\u0004\u00017\u00020871\u0004\u00018\u00022018-04-10:12:19:30\u0004\u00019\u0002J9816215233339683904\u0004\u000110\u00022018-04-10:12:19:29\u0004\u000111\u00028617120830199121\u0004\u000112\u00025000\u0004\u000113\u00020\u0004\u000114\u0003\u0004\u000115\u0003\u0004\u000116\u0003\u0004\u000117\u0003\u0004\u000118\u0003\u0004\u000119\u0003\u0004\u000120\u00020\u0004\u000121\u00020\u0004\u000122\u00020\u0004\u000123\u00020\u0004\u000124\u0002981805409554467\u0004\u000125\u0003\u0004\u000126\u0003\u0004\u000127\u0003\u0004\u000128\u000215003\u0004\u000129\u0003\u0004\u000130\u00025826\u0004\u000131\u0003\u0004\u000132\u0003\u0004\u000133\u0003\u0004\u000134\u0003\u0004\u000135\u000218669041852\u0004\u000136\u0003\u0004\u000137\u0003\u0004\u000138\u0002100006\u0004\u000139\u0003\u0004\u000140\u0003\u0004\u000141\u0003\u0004\u000142\u0003\u0004\u000143\u0003\u0004\u000144\u00024\u0004\u000145\u000286\u0004\u000146\u0003\u0004\u000147\u0003\u0004\u000148\u0003\u0004\u000149\u0003\u0004\u000150\u000220180410\u0004";
    }

    @Test
    public void oggMessageParse() throws Exception {
        OggMessage msg = OggMessageParse.parse(oggmessage.getBytes());
        System.out.println("uuid: " + new String(msg.getUuid()));
        System.out.println("scn: " + msg.getScn());
        System.out.println("oggTid: " + new String(msg.getOggTransactionId()));
        System.out.println("localTid: " + new String(msg.getLocalTransactionId()));
        System.out.println("scheme: " + new String(msg.getSchemeName()));
        System.out.println("tablename: " + new String(msg.getTableName()));
        System.out.println("Operate: " + msg.getOperate().name());
        System.out.println("Timestamp: " + msg.getTimestampInMicroSeconds());
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(msg.getTimestamp());
        System.out.println("Date: " + cal.getTime());
        List<Column> msgColumns = msg.getColumns();
        System.out.println("Columns: " + msgColumns.size());
        for (int i = msg.getColumns().size() - 1; i >= 0; i--) {
            Column c = msgColumns.get(i);
            if (c.isCurrentValueExist()) {
                if (c.getCurrentValue() != null) {
                    System.out.println("Column " + c.getIndex() + ", current value: " + new String(c.getCurrentValue()));
                } else {
                    System.out.println("Column " + c.getIndex() + ", current value [Database null]"+c.getCurrentValue());
                }
            } else {
                System.out.println("Column " + c.getIndex()+ ", current value not exists"+c.getCurrentValue());
            }

            if (c.isOldValueExist()) {
                if (c.getOldValue() != null) {
                    System.out.println("Column " + c.getIndex() + ", old value: " + new String(c.getOldValue()));
                } else {
                    System.out.println("Column " + c.getIndex()+ ", old value [Database null]");
                }
            } else {
                System.out.println("Column " + c.getIndex() + ", old value not exists"+c.getOldValue());
            }
        }
    }

    @Test
    public void tradelogParse() throws Exception {
        OggMessage msg = OggMessageParse.parse(oggmessage.getBytes());
        ITableMessageParse tableMessageParse = new TradelogTableParse();
        Map<String, Map<String, String>> tradelogMap = tableMessageParse.tableMessageParse(msg);
        System.out.println("tradelogMap="+tradelogMap);
    }
}
