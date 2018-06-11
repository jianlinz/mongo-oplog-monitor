package com.proper.enterprise.platform.mongo.oplog.monitor.notice;

import com.mongodb.DBObject;

public interface Notice {

    void handleDeletes(DBObject op);

    void handleInserts(DBObject op);

    void handleUpdates(DBObject op);

    void handleOtherOps(DBObject op);

}
