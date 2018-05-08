package com.asiainfo.oggmessage;

import java.io.Serializable;

/**
 * 0开始、1中间、2结尾、3WHOLE、4UNKNOWN等
 *
 */
public  enum OperateState implements Serializable{
    Begin(0), Middle(1), Tail(2), Whole(3), Unknown(4);
    private int tag;

    private OperateState(int tag) {
        this.tag = tag;
    }

    public static OperateState load(int tag) {
        for (OperateState ins : OperateState.values()) {
            if (ins.tag == tag) {
                return ins;
            }
        }
        return null;
    }
}