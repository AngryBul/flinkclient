package com.asiainfo.messageparse.impl;

import com.asiainfo.messageparse.inf.IXMLParse;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class XMLMessageParse implements IXMLParse{

    @Override
    public Map file2Map(String filePath) {
        String input = file2String(filePath);
        Map resultMap = new HashMap();
        try {
            Document doc = DocumentHelper.parseText(input);
            Element element =doc.getRootElement();
            element2map(element,resultMap);
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    private String file2String(String filePath)
    {
        try {
            FileReader reader = new FileReader(filePath);
            BufferedReader bufferedReader =  new BufferedReader(reader);
            String str ;
            StringBuffer sb = new StringBuffer();
            while ((str=bufferedReader.readLine())!=null)
            {
                sb.append(str);
            }
            return sb.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private void element2map(Element elmt, Map map) {
        if (null == elmt) {
            return;
        }
        String name = elmt.getName();
        if (elmt.isTextOnly()) {
            map.put(name, elmt.getText());
        } else {
            Map mapSub = new HashMap();
            List<Element> elements = (List<Element>) elmt.elements();
            for (Element elmtSub : elements) {
                element2map(elmtSub, mapSub);
            }
            Object first = map.get(name);
            if (null == first) {
                map.put(name, mapSub);
            } else {
                if (first instanceof List) {
                    ((List) first).add(mapSub);
                } else {
                    List listSub = new ArrayList();
                    listSub.add(first);
                    listSub.add(mapSub);
                    map.put(name, listSub);
                }
            }
        }
    }
}
