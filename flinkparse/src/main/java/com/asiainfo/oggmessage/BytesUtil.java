package com.asiainfo.oggmessage;

import java.io.Serializable;
import java.util.*;

/**
 * Bytes工具类(No-ThreadSafe)
 * 
 *
 */
public class BytesUtil implements Serializable{

	public final static byte LF = 10; // \n
	public final static byte DOUBLE_QUOTE = 34; // "
	public final static byte COMMA = 44; // ,
	public final static byte DASH = 45; // -
	public final static byte COLON = 58; // :
	public final static byte LEFT_BRACKET = 91; // [
	public final static byte RIGHT_BRACKET = 93; // ]
	public final static byte UNDERSCORE = 95; // _
	public final static byte LEFT_BRACE = 123; // {
	public final static byte RIGHT_BRACE = 125; // }

	public final static char HEX_DIGITS[] = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	public final static Comparator<byte[]> BYTES_ARRAY_COMPARATOR = new Comparator<byte[]>() {

		//@Override
		public int compare(byte[] o1, byte[] o2) {
			if (o1 == o2)
				return 0;
			else if (o1 == null)
				return -1;
			else if (o2 == null)
				return 1;

			int i = 0, stop = Math.min(o1.length, o2.length);
			while (i < stop && o1[i] == o2[i])
				i++;

			if (i < stop) {
				return o1[i] - o2[i];
			} else if (o1.length == o2.length) {
				return o1[i - 1] - o2[i - 1];
			}
			return i < o1.length ? 1 : -1;
		}
	};

	/**
	 * 使用Arrays.sort进行原地排序
	 * 
	 * @param content
	 */
	public static void sort(byte[][] content) {
		if (content == null)
			return;
		Arrays.sort(content, 0, content.length, BYTES_ARRAY_COMPARATOR);
	}

	/**
	 * 对排序后的二维数组进行二分查找
	 * 
	 * @param content
	 * @param searchKey
	 * @return
	 */
	public static int binarySearch(byte[][] content, byte[] searchKey) {
		if (content == null)
			return -1;
		return Arrays.binarySearch(content, searchKey, BYTES_ARRAY_COMPARATOR);
	}

	/**
	 * 使用hashCode为键值进行hash查找(不要求也不进行排序)
	 * 
	 * @param content
	 * @param searchKey
	 * @return
	 */
	public static int hashSearch(byte[][] content, byte[] searchKey) {
		if (content == null)
			return -1;
		Map<Bytes, Integer> map = hashMap(content);
		// return map.getOrDefault(new Bytes(searchKey), -1);
		Bytes key = new Bytes(searchKey);
		if (map.containsKey(key)) {
			map.get(key);
		}
		return -1;
	}

	public static boolean equals(byte[] a, byte[] b) {
		return BYTES_ARRAY_COMPARATOR.compare(a, b) == 0;
	}

	public static int compare(byte[] a, byte[] b) {
		return BYTES_ARRAY_COMPARATOR.compare(a, b);
	}

	/**
	 * 前缀检测
	 * 
	 * @param content
	 * @param prefix
	 * @return
	 */
	public static boolean startsWith(byte[] content, byte[] prefix) {
		if (content == prefix)
			return true;
		if (content == null || prefix == null || prefix.length > content.length) {
			return false;
		}
		int i = 0, stop = prefix.length;
		while (i < stop && prefix[i] == content[i])
			i++;
		return i == stop;
	}

	/**
	 * 后缀检测
	 * 
	 * @param content
	 * @param suffix
	 * @return
	 */
	public static boolean endsWith(byte[] content, byte[] suffix) {
		if (content == suffix)
			return true;
		if (content == null || suffix == null || suffix.length > content.length) {
			return false;
		}
		int i = 0, stop = suffix.length;
		int j = content.length - stop;
		for (; i < stop && suffix[i] == content[j]; i++, j++)
			;
		return i == stop;
	}

	/**
	 * @param content
	 * @param searchKey
	 * @return
	 */
	public static int indexOf(byte[] content, byte[] searchKey) {
		return indexOf(content, 0, content.length, searchKey);
	}

	/**
	 * 
	 * @param content
	 * @param start
	 * @param length
	 * @param searchKey
	 * @return
	 */
	public static int indexOf(byte[] content, int start, int length,
			byte[] searchKey) {
		byte first = searchKey[0];
		int stop = Math.min(start + length, content.length) - searchKey.length
				+ 1;
		for (int i = start; i < stop; i++) {
			if (content[i] != first) {
				while (++i < stop && content[i] != first)
					;
			}
			if (i < stop) {
				int j = i + 1, jstop = i + searchKey.length;
				for (int k = 1; j < jstop && content[j] == searchKey[k]; k++, j++)
					;
				if (j == jstop)
					return j - searchKey.length;
			}
		}
		return -1;
	}

	/**
	 * 统计b出现的次数
	 * 
	 * @param content
	 * @param b
	 * @return
	 */
	public static int count(byte[] content, byte b) {
		if (content == null || content.length == 0)
			return 0;
		int total = 0;
		for (byte i : content) {
			if (i == b) {
				total++;
			}
		}
		return total;
	}

	/**
	 * 
	 * 切分数组, 注意如果分割符出现的次数少于切分的次数, 返回的数据个数为实际可以分割的个数
	 * 
	 * @param data
	 *            待分割数数组
	 * @param b
	 *            分割字符
	 * @param splitTimes
	 *            切分的次数 0表示不做次数限制
	 * @return
	 */
	public static byte[][] split(byte[] data, byte b, int splitTimes) {
		if (data == null) {
			return new byte[0][];
		}
		List<byte[]> container = splitList(data, b, splitTimes);
		byte[][] ret = new byte[container.size()][];
		container.toArray(ret);
		return ret;
	}

	/**
	 * 
	 * 切分数组, 注意如果分割符出现的次数少于切分的次数, 返回的数据个数为实际可以分割的个数
	 * 
	 * @param data
	 *            待分割数数组
	 * @param b
	 *            分割字符
	 * @param splitTimes
	 *            切分的次数 0表示不做次数限制
	 * @return
	 */
	public static List<byte[]> splitList(byte[] data, byte b, int splitTimes) {
		List<byte[]> container = new LinkedList<byte[]>();
		if (data == null) {
			return container;
		}
		int count = 0, start = 0, index = 0;
		byte[] empty = new byte[0];
		for (byte i : data) {
			if (i == b) {
				if (index == start) {
					container.add(empty);
				} else {
					byte[] one = new byte[index - start];
					System.arraycopy(data, start, one, 0, one.length);
					container.add(one);
				}
				start = index + 1;
				count++;
			}
			if (splitTimes > 0 && count >= splitTimes) {
				break;
			}
			index++;
		}
		if (start < data.length) {
			byte[] one = new byte[data.length - start];
			System.arraycopy(data, start, one, 0, one.length);
			container.add(one);
		} else if (splitTimes <= 0 || count <= splitTimes) {
			container.add(empty);
		}
		return container;
	}

	/**
	 * 根据分隔符分割字节串, 分隔符有多个，用索引标示
	 * 
	 * @param data
	 * @param indexSeps
	 *            用索引标示的分隔符, 值为1则该索引是分隔符
	 * @return
	 */
	public static List<byte[][]> splitMulSeps(byte[] data, byte[] indexSeps) {

		LinkedList<byte[][]> ret = new LinkedList<byte[][]>();
		if (data == null || data.length == 0) {
			return ret;
		}

		int index = 0, start = 0;
		byte tag = 0;

		for (byte cur : data) {
			if (cur >= 0 && indexSeps[cur] == 1) {
				byte[] content = new byte[index - start];
				System.arraycopy(data, start, content, 0, content.length);

				byte[][] item = new byte[2][];
				item[0] = new byte[] { tag };
				item[1] = content;

				ret.add(item);

				start = index + 1;
				tag = cur;
			}
			index++;
		}

		byte[] content = new byte[index - start];
		System.arraycopy(data, start, content, 0, content.length);

		byte[][] item = new byte[2][];
		item[0] = new byte[] { tag };
		item[1] = content;

		ret.add(item);

		return ret;
	}

	/**
	 * 
	 * @param bsa
	 *            为null则返回null
	 * @return 返回hashCode为键, 索引(数组下标)为值的HashMap
	 */
	public static Map<Bytes, Integer> hashMap(byte[][] bsa) {
		if (bsa == null)
			return null;
		Map<Bytes, Integer> map = new HashMap<Bytes, Integer>(bsa.length);
		for (int i = bsa.length; i-- > 0;) {
			map.put(new Bytes(bsa[i]), i);
		}
		return map;
	}

	/**
	 * 和 {@Code Arrays.hashCode}一样,
	 * 类似String.hashCode除了null对象也有hashCode之外(方便byte[][]区分null和空byte数组)
	 * 
	 * @param bs
	 * @return
	 */
	public static int hashCode(byte[] bs) {
		if (bs == null)
			return 0;
		int hash = 1;
		for (byte i : bs) {
			hash = hash * 31 + i;
		}
		return hash;
	}

	public static byte[] toBytes(long n) {
		if (n == 0)
			return new byte[] { '0' };
		byte[] bytes = new byte[21]; // 9223372036854775808
		int i = bytes.length;
		long p = Math.abs(n);
		while (p > 0) {
			bytes[--i] = (byte) ((p % 10) + 48);
			p = p / 10;
		}
		if (n < 0) {
			bytes[--i] = '-';
		}
		byte[] ret = new byte[bytes.length - i];
		System.arraycopy(bytes, i, ret, 0, ret.length);
		return ret;
	}

	public static byte[] toBytes(int n) {
		if (n == 0)
			return new byte[] { '0' };
		byte[] bytes = new byte[11]; // 2 147 483 648
		int p = Math.abs(n), i = bytes.length;
		while (p > 0) {
			bytes[--i] = (byte) ((p % 10) + 48);
			p = p / 10;
		}
		if (n < 0) {
			bytes[--i] = '-';
		}
		byte[] ret = new byte[bytes.length - i];
		System.arraycopy(bytes, i, ret, 0, ret.length);
		return ret;
	}

	public static long parseLong(byte[] bytes) {
		if (bytes == null || bytes.length == 0)
			return 0;
		int start = bytes[0] == '-' || bytes[0] == '+' ? 1 : 0;
		long r = 0;
		for (int i = start; i < bytes.length; i++) {
			r = r * 10 + (bytes[i] - 48);
		}
		return start == 0 || bytes[0] == '+' ? r : -r;
	}

	public static int parseInt(byte[] bytes) {
		Long r = parseLong(bytes);
		return r.intValue();
	}

	public static double parseDouble(byte[] bytes) {
		if (bytes == null || bytes.length == 0)
			return 0;
		int start = bytes[0] == '-' || bytes[0] == '+' ? 1 : 0;
		double fn = 0;
		int i = start;
		while (i < bytes.length && bytes[i] != '.') {
			fn = fn * 10 + (bytes[i] - 48);
			i++;
		}
		int ip = i++;
		while (i < bytes.length) {
			fn += (bytes[i] - 48) / Math.pow(10, i - ip);
			i++;
		}
		return start == 0 || bytes[0] == '+' ? fn : -fn;
	}

	/**
	 * 根据是否含有小数点符号返回long和double两种类型 (注意越界)
	 * 
	 * @param bytes
	 * @return
	 */
	public static Number parseNumber(byte[] bytes) {
		if (bytes == null || bytes.length == 0)
			return 0;
		int start = bytes[0] == '-' || bytes[0] == '+' ? 1 : 0;
		long in = 0;
		double fn = 0;
		int i = start;

		while (i < bytes.length && bytes[i] != '.') {
			in = in * 10 + (bytes[i] - 48);
			i++;
		}
		int ip = i++;
		while (i < bytes.length) {
			fn += (bytes[i] - 48) / Math.pow(10, i - ip);
			i++;
		}
		int sign = start == 0 || bytes[0] == '+' ? 1 : -1;
		return sign * (ip >= bytes.length ? in : fn + in);
	}

	public static String string(byte[] bs) {
		if (bs == null)
			return "null";
		return new String(bs);
	}

	public static String string(byte[][] content, String sep) {
		if (content == null)
			return null;
		else if (content.length == 0)
			return "";
		StringBuilder builder = new StringBuilder(
				content[0] != null ? new String(content[0]) : "null");
		for (int i = 1; i < content.length; i++) {
			builder.append(sep).append(
					content[i] != null ? new String(content[i]) : "null");
		}
		return builder.toString();
	}

	public static String string(Map<? extends Object, ? extends Object> map,
                                String sep) {
		if (map == null)
			return null;
		else if (map.size() == 0)
			return "";
		StringBuilder builder = new StringBuilder();

		Set<? extends Object> keys = map.keySet();

		for (Iterator<? extends Object> iter = keys.iterator(); iter.hasNext();) {
			Object key = iter.next();
			Object value = map.get(key);
			builder.append(key).append(":");
			builder.append(value).append(sep);
		}
		return builder.toString();
	}

	public static String hexliteralString(byte[] bs) {
		if (bs == null)
			return "null";
		char[] chars = new char[bs.length << 2];
		int i = 0;
		for (byte b : bs) {
			chars[i++] = '\\'; // 0x5c
			chars[i++] = 'x';
			chars[i++] = HEX_DIGITS[b >>> 4 & 0xf];
			chars[i++] = HEX_DIGITS[b & 0xf];
		}
		return new String(chars);
	}

	public static void main(String[] args) {
		// System.out.println(indexOf("abcdefghijklm".getBytes(), 9, 4,
		// "klm".getBytes()));
		byte[] t = toBytes(791);
		System.out.println(BytesUtil.compare("xiao".getBytes(),
				"xiao".getBytes()));
		System.out.println(hexliteralString(t));
		System.out.println(parseDouble("0".getBytes()));
		System.out.println(parseDouble("0.1".getBytes()));
		System.out.println(parseInt("67".getBytes()));
		System.out.println(parseDouble("-67.0".getBytes()));
		System.out.println(parseDouble("67.1".getBytes()));
		System.out.println(parseDouble("67.45613".getBytes()));

		System.out.println("---------------");
		String s = "AB,CD::EF,KG::";
		byte[][] p = BytesUtil.split(s.getBytes(), BytesUtil.COLON, 6);
		System.out.println("--:" + p.length);
		for (byte[] i : p) {
			System.out.println(i.length + "-" + new String(i));
		}

	}

}
