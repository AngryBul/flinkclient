package com.asiainfo.messageparse.impl;

import com.asiainfo.oggmessage.OggMessage;

import java.util.Map;

public class UserTableParse extends OggMessageParse{


    @Override
    public Map<String, Map<String, String>> tableMessageParse(OggMessage oggMessage) {
        return null;
    }

    @Override
    public boolean checkDate(String str) {
        return false;
    }
}
