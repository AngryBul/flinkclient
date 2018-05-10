package com.asiainfo.messageparse.test;

import com.asiainfo.messageparse.impl.RealFeeParse;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class XMLParseTest {

    private String filePath;

    @Before
    public void init()
    {
        filePath="C:\\Users\\zhanghao\\Documents\\Tencent Files\\455306073\\FileRecv\\20180430_99_RealFee_3100_0001.REQ";
    }

    @Test
    public void testXMLParse()
    {
        Map map = new RealFeeParse().file2Map(filePath);
        System.out.println(map);
    }
}
