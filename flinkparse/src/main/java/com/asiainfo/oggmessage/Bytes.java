package com.asiainfo.oggmessage;

import java.io.Serializable;

/**
 * 
 * byte[] wrapper, for Map key
 * 
 *
 */
public class Bytes implements Serializable{

	/**
	 * 修改数组中的元素使用change方法, 直接修改将导致Bug
	 */
	public final byte[] bytes;

	private int hash = 0; // initial value 0

	public Bytes(byte[] bytes) {
		this.bytes = bytes;
	}

	/**
	 * 修改byte数组中的元素
	 * 
	 * @param index
	 * @param element
	 * @return 修改后的byte[] 数组
	 */
	public byte[] change(int index, byte element) {
		bytes[index] = element;
		hash = 0;
		return bytes;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj || obj == bytes) {
			return true;
		} else if (obj == null || bytes == null) {
			return false;
		}
		byte[] data;
		if (obj instanceof Bytes) {
			Bytes oth = (Bytes) obj;
			if (oth.hashCode() != hashCode()) {
				return false;
			}
			data = oth.bytes;
		} else if (obj instanceof byte[]) {
			data = (byte[]) obj;
		} else {
			return false;
		}
		if (data.length == bytes.length) {
			int i = data.length;
			while (i-- > 0 && data[i] == bytes[i])
				;
			return i < 0;
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (bytes == null || hash != 0)
			return hash;
		int h = 1;
		for (byte i : bytes) {
			h = h * 31 + i;
		}
		hash = h;
		return h;
	}

	@Override
	public String toString() {
		return bytes != null ? new String(bytes) : "null";
	}

	public int length() {
		return bytes != null ? bytes.length : 0;
	}

}
