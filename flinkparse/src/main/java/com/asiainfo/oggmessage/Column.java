package com.asiainfo.oggmessage;

import java.io.Serializable;
import java.util.Arrays;

public  class Column implements Serializable {
    public int index;
    public byte[] name;
    public byte[] currentValue;
    public byte[] oldValue;
    private boolean currentValueExist = true;
    private boolean oldValueExist = true;


    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public byte[] getName() {
        return name;
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public byte[] getCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(byte[] currentValue) {
        this.currentValue = currentValue;
    }

    public byte[] getOldValue() {
        return oldValue;
    }

    public void setOldValue(byte[] oldValue) {
        this.oldValue = oldValue;
    }

    public boolean isCurrentValueExist() {
        return currentValueExist;
    }

    public void setCurrentValueExist(boolean currentValueExist) {
        this.currentValueExist = currentValueExist;
    }

    public boolean isOldValueExist() {
        return oldValueExist;
    }

    public void setOldValueExist(boolean oldValueExist) {
        this.oldValueExist = oldValueExist;
    }

		/*@Override
		public String toString() {
			return "Column{" +
					"index=" + index +
					", name=" +  new String(name) +
					", currentValue=" + new String(currentValue) +
					", oldValue=" + new String(oldValue) +
					", currentValueExist=" + currentValueExist +
					", oldValueExist=" + oldValueExist +
					'}';
		}*/

    @Override
    public String toString() {
        return "Column{" +
                "index=" + index +
                ", name=" + Arrays.toString(name) +
                ", currentValue=" + Arrays.toString(currentValue) +
                ", oldValue=" + Arrays.toString(oldValue) +
                ", currentValueExist=" + currentValueExist +
                ", oldValueExist=" + oldValueExist +
                '}';
    }
}
