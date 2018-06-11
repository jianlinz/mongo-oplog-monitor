package com.proper.enterprise.platform.mongo.oplog.monitor;

import org.junit.Test;


public class OplogMonitorTest {

    @Test
    public void start() throws InterruptedException {
        OplogMonitor.start("127.0.0.1:27017", new NoticeServiceImpl(), true);
        Thread.sleep(1000000);
    }
}
