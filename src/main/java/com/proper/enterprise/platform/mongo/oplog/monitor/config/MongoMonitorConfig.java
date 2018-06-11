package com.proper.enterprise.platform.mongo.oplog.monitor.config;

import com.mongodb.*;
import com.proper.enterprise.platform.mongo.oplog.monitor.notice.Notice;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static java.util.Collections.singletonList;

public class MongoMonitorConfig {

    private MongoMonitorConfig() {
    }

    private MongoMonitorConfig(String host, String userName, String password, Notice notice, boolean restart) {
        if (StringUtils.isEmpty(host)) {
            throw new RuntimeException("host cant't be empty");
        }
        if (null == notice) {
            throw new RuntimeException("notice cant't be null");
        }
        this.notice = notice;
        this.host = host;
        this.userName = userName;
        this.password = password;
        this.restart = restart;
    }

    public static final String TIME_DB_NAME = "monitor_time_d";

    private MongoClient hostMongo;
    private Map<String, MongoClient> shardSetClients;
    private DB timeDB;
    private String host;
    private String userName;
    private String password;
    private Notice notice;
    private boolean restart;

    private static MongoMonitorConfig single;

    public static MongoMonitorConfig getInstance(String host, String userName, String password, Notice notice, boolean restart) {
        if (single == null) {
            synchronized (MongoMonitorConfig.class) {
                if (single == null) {
                    single = new MongoMonitorConfig(host, userName, password, notice, restart);
                    initConfig();
                }
            }
        }
        return single;
    }

    public static MongoMonitorConfig getMongoMonitorConfig() {
        if (single == null) {
            synchronized (MongoMonitorConfig.class) {
                if (single == null) {
                    throw new RuntimeException("MongoMonitorConfig is null");
                }
            }
        }
        return single;
    }

    private static void initConfig() {
        initClient();
        initShardSetClients();
        initTimeDB();
    }

    private static void initClient() {
        List<ServerAddress> serverAddresses = new ArrayList<>();
        for (String address : single.host.split(",")) {
            serverAddresses.add(new ServerAddress(address));
        }
        single.hostMongo = new MongoClient(serverAddresses, getMongoCredential(single.userName, single.password));
    }

    private static List<MongoCredential> getMongoCredential(String userName, String password) {
        List<MongoCredential> credentialList = Collections.emptyList();
        if (StringUtils.isNotBlank(userName)) {
            credentialList = singletonList(MongoCredential.createCredential(userName,
                    "admin", password.toCharArray()));
        }
        return credentialList;
    }

    private static void initShardSetClients() {
        single.shardSetClients = new ShardSetFinder().findShardSets(single.hostMongo);
    }

    private static void initTimeDB() {
        single.timeDB = single.hostMongo.getDB(TIME_DB_NAME);
    }

    private static class ShardSetFinder {

        public Map<String, MongoClient> findShardSets(MongoClient mongoS) {
            DBCursor find = mongoS.getDB("admin").getSisterDB("config")
                    .getCollection("shards").find();
            Map<String, MongoClient> shardSets = new HashMap<>();
            while (find.hasNext()) {
                DBObject next = find.next();
                System.out.println("Adding " + next);
                String key = (String) next.get("_id");
                shardSets.put(key, getMongoClient(buildServerAddressList(next)));
            }
            if (shardSets.size() == 0) {
                shardSets.put("single", single.hostMongo);
            }
            find.close();
            return shardSets;
        }

        private MongoClient getMongoClient(List<ServerAddress> shardSet) {
            MongoClientOptions opts = new MongoClientOptions.Builder().readPreference(ReadPreference.primary()).build();
            return new MongoClient(shardSet, getMongoCredential(single.userName, single.password), opts);
        }

        private List<ServerAddress> buildServerAddressList(DBObject next) {
            List<ServerAddress> hosts = new ArrayList<>();
            for (String host : Arrays
                    .asList(((String) next.get("host")).split("/")[1].split(","))) {
                hosts.add(new ServerAddress(host));
            }
            return hosts;
        }
    }

    public MongoClient getHostMongo() {
        return hostMongo;
    }

    public Map<String, MongoClient> getShardSetClients() {
        return shardSetClients;
    }

    public DB getTimeDB() {
        return timeDB;
    }


    public Notice getNotice() {
        return notice;
    }

    public boolean isRestart() {
        return restart;
    }
}
