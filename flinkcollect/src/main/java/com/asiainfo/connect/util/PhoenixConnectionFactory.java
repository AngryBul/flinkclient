package com.asiainfo.connect.util;


public class PhoenixConnectionFactory{

    private static PhoenixConnectionPool phoenixConnection = null;

    private PhoenixConnectionFactory()
    {
    }

    public static PhoenixConnectionPool createConnnectionPool(String zkUrl)
    {
        if(phoenixConnection == null)
        {
            phoenixConnection = new PhoenixConnectionPool(zkUrl);
        }
        try {
            phoenixConnection.createPool();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("phoenix 连接创建异常.");
        }
        return phoenixConnection;
    }

}
