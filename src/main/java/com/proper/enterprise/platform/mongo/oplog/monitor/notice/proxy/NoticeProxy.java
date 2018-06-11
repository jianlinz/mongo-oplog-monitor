package com.proper.enterprise.platform.mongo.oplog.monitor.notice.proxy;

import com.mongodb.DBObject;
import com.proper.enterprise.platform.mongo.oplog.monitor.notice.Notice;

public class NoticeProxy implements Notice {

    public NoticeProxy(Notice notice) {
        this.notice = notice;
    }

    private Notice notice;

    @Override
    public void handleDeletes(DBObject op) {
        notice.handleDeletes(op);
    }

    @Override
    public void handleInserts(DBObject op) {
        notice.handleInserts(op);
    }

    @Override
    public void handleUpdates(DBObject op) {
        notice.handleUpdates(op);
    }

    @Override
    public void handleOtherOps(DBObject op) {
        notice.handleOtherOps(op);
    }

}
