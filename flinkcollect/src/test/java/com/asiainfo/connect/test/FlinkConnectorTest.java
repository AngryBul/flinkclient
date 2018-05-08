package com.asiainfo.connect.test;

import com.asiainfo.connect.factory.FlinkConnectFactory;
import org.junit.Before;

public class FlinkConnectorTest {

    private String jobId;

    @Before
    public void init()
    {
        jobId="1";
    }

    public void testConnector() throws Exception {

        new FlinkConnectFactory.StreamConnector().createJob(jobId);
    }
}
