package com.asiainfo.oggmessage;

import java.io.Serializable;

/**
 * 数据操作 类型
 */
public enum Operate implements Serializable{

    Update((byte) 'U'), Insert((byte) 'I'), Delete((byte) 'D'), Key((byte) 'K');

    private byte tag;

    private Operate(byte tag) {
        this.tag = tag;
    }

    public static Operate load(byte tag) {
        for (Operate ins : Operate.values()) {
            if (ins.tag == tag) {
                return ins;
            }
        }
        return null;
    }
}