package com.asiainfo.messageparse.impl;

import com.asiainfo.messageparse.inf.ITableMessageParse;
import com.asiainfo.oggmessage.*;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * 
 * OGG KAFKA Message(V0.2.1-2) Parser
 * 
 * @date 2016-02-23
 *
 */
public abstract class OggMessageParse implements ITableMessageParse {

    public static Logger log = Logger.getLogger(OggMessageParse.class.getName());

	public static final SimpleDateFormat YMD_HMS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static final int TZ = 8; // Time Zone

	/**
	 * 一级分隔符，分割消息,得到一级元素
	 */
	public final static byte FIRST_SEPERATOR = 0x01;

	/**
	 * 二级分割符, 分割一级元素, 获取子元素, 用于非列项
	 */
	public final static byte SECOND_SEPERATOR = 0x02;

	/**
	 * 值分隔符, 也是值类型标示, 仅支持正数
	 */
	public final static byte[] VALUE_SEPERATORS = new byte[] { 0x02, 0x03, 0x04 };

	/**
	 * 一级元素最小个数
	 */
	public final static short MIN_FIRST_PARTS = 4;

    /**
     * 供优化速度
     */
    private final static byte[] _VALUE_SEPERATORS = new byte[1 + Byte.MAX_VALUE];

    static {
        for (byte b : VALUE_SEPERATORS) {
            _VALUE_SEPERATORS[b] = 1;
        }
    }


	/**
	 * 解析一个Ogg消息， 注意一个Kafka消息可能包含多个Ogg消息
	 * 
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static OggMessage parse(byte[] data) throws Exception {
		List<byte[]> parts = BytesUtil.splitList(data, FIRST_SEPERATOR, 0);
		if (parts.size() < MIN_FIRST_PARTS) {
			throw new Exception(String.format("message parts only has %s, less %s， message size: %s", parts.size(),
					MIN_FIRST_PARTS, data == null ? "null" : data.length));
		}

		long scn = 0;
		List<byte[]> partA0 = splitAndCheck(parts, 0, SECOND_SEPERATOR, 5);
		List<byte[]> partA1 = BytesUtil.splitList(parts.get(1), (byte) '.', 0);

		if (parts.get(2) == null || parts.get(2).length == 0) {
			throw new Exception("get operate failed: empty");
		}

		if (partA1.size() < 2 || partA1.size() > 3) {
			throw new Exception("parser [CATALOG_NAME.]SCHEMA_NAME.TABLE_NAME failed: " + new String(parts.get(1)));
		}

		byte[] temp = partA0.get(1);
		for (int i = 0, L = temp != null ? temp.length : 0; i < L; i++) {
			scn = scn * 10 + temp[i] - 48;
		}

		OperateState opState = OperateState.load(partA0.get(4)[0] - 48);
		if (opState == null) {
			throw new Exception("get operate state failed, unkown: " + new String(partA0.get(4)));
		}

		Operate op = Operate.load(parts.get(2)[0]);
		if (op == null) {
			throw new Exception("get operate failed, unkown: " + new String(parts.get(2)));
		}
		temp = parts.get(3);
		if (temp == null || temp.length < 19) { // yyyy-MM-dd HH:mm:ss.SSSSSS
			throw new Exception("get operate time failed, unkown: " + new String(parts.get(3)));
		}

		// 从UTC时间转换到本地时间
		long timestampInMicro = parseDate(temp) + TZ * 3600000000L;

		List<Column> columns = new LinkedList<Column>();
		for (int i = 4; i < parts.size(); i++) {
			Column col = parseColumn(parts, i);
			if (col != null) {
				columns.add(col);
			}
		}

		OggMessage aOgg = new OggMessage();
		// 第一项
		aOgg.setUuid(partA0.get(0));
		aOgg.setScn(scn);
		aOgg.setOggTransactionId(partA0.get(2));
		aOgg.setLocalTransactionId(partA0.get(3));
		aOgg.setOpState(opState);

		// 第二项
		if (partA1.size() == 2) {
			aOgg.setSchemeName(partA1.get(0));
			aOgg.setTableName(partA1.get(1));
		} else {
			aOgg.setCatalogName(partA1.get(0));
			aOgg.setSchemeName(partA1.get(1));
			aOgg.setTableName(partA1.get(2));
		}

		// 第三项
		aOgg.setOperate(op);

		// 第四项
		aOgg.setTimestampInMicroSeconds(timestampInMicro);

		// 各列值
		aOgg.setColumns(columns);
		return aOgg;
	}

	public static long parseDate(byte[] temp) {
		// yyyy-MM-dd HH:mm:ss.SSSSSS
		int year, month, day, hour, minute, second, microsecond = 0;
		year = (temp[0] - 48) * 1000 + (temp[1] - 48) * 100 + (temp[2] - 48) * 10 + temp[3] - 48;
		month = (temp[5] - 48) * 10 + temp[6] - 48;
		day = (temp[8] - 48) * 10 + temp[9] - 48;
		hour = (temp[11] - 48) * 10 + temp[12] - 48;
		minute = (temp[14] - 48) * 10 + temp[15] - 48;
		second = (temp[17] - 48) * 10 + temp[18] - 48;

		for (int i = 20; i < temp.length; i++) {
			microsecond = microsecond * 10 + temp[i] - 48;
		}
		for (int i = 26 - temp.length; i > 0; i--) { // 不足6位 补齐
			microsecond = microsecond * 10;
		}
		long ret = 0;
		if (year > 2000) { // for speed
			int days = day;
			for (int i = 2000; i < year; i++) {
				days += (i % 400 == 0 || i % 4 == 0 && i % 100 != 0) ? 366 : 365;
			}
			int[] DAYS_BEFORE_MONTH = new int[] { 0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 };
			days += DAYS_BEFORE_MONTH[month];
			if (month < 3 || year % 4 != 0 || year % 100 == 0 && year % 400 != 0) {
				days--;
			}
			ret = 946656000000L + (days * 86400 + hour * 3600 + minute * 60 + second) * 1000L;
		} else {
			Calendar calendar = Calendar.getInstance();
			calendar.set(year, month - 1, day, hour, minute, second);
			calendar.set(Calendar.MILLISECOND, 0);
			ret = calendar.getTimeInMillis();
		}
		return ret * 1000 + microsecond;
	}

	/**
	 * 根据分隔符分割字节串, 并进行个数检查
	 * 
	 * @param data
	 * @param index
	 * @param sep
	 * @param size
	 * @return
	 * @throws Exception
	 */
	private static List<byte[]> splitAndCheck(List<byte[]> data, int index, byte sep, int size) throws Exception {
		List<byte[]> parts = BytesUtil.splitList(data.get(index), sep, size > 1 ? size - 1 : 0);
		if (size > 0 && parts.size() < size) {
			throw new Exception(String.format("part %s only has %s, less %s", index, parts.size(), size));
		}
		return parts;
	}

	/**
	 * 解析Ogg消息里的列
	 * 
	 * @param parts
	 * @param i
	 * @return
	 * @throws Exception
	 */
	private static Column parseColumn(List<byte[]> parts, int i) throws Exception {
		byte[] data = parts.get(i);
		byte[][] sepData = null;

		List<byte[][]> columnParts = BytesUtil.splitMulSeps(data, _VALUE_SEPERATORS);

		if (columnParts.size() == 0) {
			return null;
		}

		if (columnParts.size() < 3) {
			throw new Exception(String.format("column %s only has %s, less %s", i, columnParts.size(), 3));
		}

		Column aColumn = new Column();
		byte[] bIndex = columnParts.get(0)[1];
		int v = 0;
		for (int j = 0; j < bIndex.length; j++) {
			v = v * 10 + bIndex[j] - 48;
		}
		aColumn.setIndex(v);
		sepData = columnParts.get(1);
		if (sepData[0][0] == 0x02) {
			aColumn.setCurrentValue(sepData[1]);
		} else if (sepData[0][0] == 0x03) {
			aColumn.setCurrentValue(null);
		} else if (sepData[0][0] == 0x04) {
			aColumn.setCurrentValueExist(false);
		}

		sepData = columnParts.get(2);
		if (sepData[0][0] == 0x02) {
			aColumn.setOldValue(sepData[1]);
		} else if (sepData[0][0] == 0x03) {
			aColumn.setOldValue(null);
		} else if (sepData[0][0] == 0x04) {
			aColumn.setOldValueExist(false);
		}

		return aColumn;
	}


	public abstract Map<String,Map<String,String>> tableMessageParse(OggMessage oggMessage);

}
