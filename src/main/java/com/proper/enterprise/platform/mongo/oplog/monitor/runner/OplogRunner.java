package com.proper.enterprise.platform.mongo.oplog.monitor.runner;

import com.mongodb.*;
import com.proper.enterprise.platform.mongo.oplog.monitor.config.MongoMonitorConfig;
import com.proper.enterprise.platform.mongo.oplog.monitor.notice.handle.NoticeHandle;
import org.bson.types.BSONTimestamp;

import java.util.Date;
import java.util.Map.Entry;

public class OplogRunner implements Runnable {

    private MongoClient client;
    private BSONTimestamp lastTimeStamp;
    private DBCollection shardTimeCollection;

    public OplogRunner(Entry<String, MongoClient> client, DB timeDB) {
        this.client = client.getValue();
        shardTimeCollection = timeDB.getCollection(client.getKey());
        if (MongoMonitorConfig.getMongoMonitorConfig().isRestart()) {
            lastTimeStamp = new BSONTimestamp(getSecondTimestampTwo(new Date()), 1);
            return;
        }
        DBObject findOne = shardTimeCollection.findOne();
        if (findOne != null) {
            lastTimeStamp = (BSONTimestamp) findOne.get("ts");
        }
    }

    @Override
    public void run() {
        DBCollection fromCollection = client.getDB("local").getCollection("oplog.rs");
        DBObject timeQuery = getTimeQuery();
        DBCursor opCursor = fromCollection.find(timeQuery)
                .sort(new BasicDBObject("$natural", 1))
                .addOption(Bytes.QUERYOPTION_TAILABLE)
                .addOption(Bytes.QUERYOPTION_AWAITDATA)
                .addOption(Bytes.QUERYOPTION_NOTIMEOUT);
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                continue;
            }
            if (opCursor.hasNext()) {
                DBObject nextOp = opCursor.next();
                lastTimeStamp = ((BSONTimestamp) nextOp.get("ts"));
                shardTimeCollection.update(new BasicDBObject(),
                        new BasicDBObject("$set", new BasicDBObject("ts",
                                lastTimeStamp)), true, true, WriteConcern.SAFE);
                NoticeHandle.notice(nextOp);
            }
        }

    }

    private DBObject getTimeQuery() {
        return lastTimeStamp == null ? new BasicDBObject() : new BasicDBObject("ts", new BasicDBObject("$gt", lastTimeStamp));
    }

    private int getSecondTimestampTwo(Date date) {
        if (null == date) {
            return 0;
        }
        String timestamp = String.valueOf(date.getTime() / 1000);
        return Integer.valueOf(timestamp);
    }
}
