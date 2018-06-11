package com.proper.enterprise.platform.mongo.oplog.monitor.notice.handle;

import com.mongodb.DBObject;
import com.proper.enterprise.platform.mongo.oplog.monitor.config.MongoMonitorConfig;
import com.proper.enterprise.platform.mongo.oplog.monitor.notice.Notice;
import com.proper.enterprise.platform.mongo.oplog.monitor.notice.proxy.NoticeProxy;

public class NoticeHandle {

    private NoticeHandle() {
    }

    public static void notice(DBObject op) {
        Notice notice = MongoMonitorConfig.getMongoMonitorConfig().getNotice();
        Notice noticeProxy = new NoticeProxy(notice);
        switch ((String) op.get("op")) {
            case "u":
                if (!op.get("ns").toString().contains(MongoMonitorConfig.TIME_DB_NAME)) {
                    noticeProxy.handleUpdates(op);
                }else {
                    System.out.println(op);
                }
                break;
            case "i":
                noticeProxy.handleInserts(op);
                break;
            case "d":
                noticeProxy.handleDeletes(op);
                break;
            default:
                noticeProxy.handleOtherOps(op);
                break;
        }
    }
}
