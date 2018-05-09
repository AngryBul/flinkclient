package com.asiainfo.connect.test;

import com.asiainfo.connect.factory.FlinkConnectFactory;
import org.junit.Before;
import org.junit.Test;

public class FlinkConnectorTest {

    private String jobId;

    @Before
    public void init()
    {
        jobId="1";
    }

    @Test
    public void testConnector() throws Exception {

        new FlinkConnectFactory.StreamConnector().createJob(jobId);
    }
}
