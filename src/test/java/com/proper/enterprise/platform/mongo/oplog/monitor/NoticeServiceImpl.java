package com.proper.enterprise.platform.mongo.oplog.monitor;

import com.mongodb.DBObject;
import com.proper.enterprise.platform.mongo.oplog.monitor.notice.Notice;

public class NoticeServiceImpl implements Notice {
    @Override
    public void handleDeletes(DBObject op) {
        System.out.println(op);
    }

    @Override
    public void handleInserts(DBObject op) {
        System.out.println(op);
    }

    @Override
    public void handleUpdates(DBObject op) {
        System.out.println(op);
    }

    @Override
    public void handleOtherOps(DBObject op) {
        System.out.println(op);
    }
}
