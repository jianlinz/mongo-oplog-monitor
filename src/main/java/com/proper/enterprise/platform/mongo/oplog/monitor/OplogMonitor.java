package com.proper.enterprise.platform.mongo.oplog.monitor;

import com.mongodb.MongoClient;
import com.proper.enterprise.platform.mongo.oplog.monitor.config.MongoMonitorConfig;
import com.proper.enterprise.platform.mongo.oplog.monitor.notice.Notice;
import com.proper.enterprise.platform.mongo.oplog.monitor.runner.OplogRunner;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OplogMonitor {


    public static void start(String host, Notice notice, boolean restart) {
        start(host, "", "", notice, restart);
    }

    /**
     * 启动入口方法
     *
     * @param host     地址
     * @param userName 用户名
     * @param password 密码
     * @param notice   通知接口具体实现
     * @param restart  是否根据当前时间读取   当程序down了以后 restart 为false则会将漏掉的日志继续重读一遍  若restart为true则只读当前时间以后的新日志
     */
    public static void start(String host, String userName, String password, Notice notice, boolean restart) {
        MongoMonitorConfig.getInstance(host, userName, password, notice, restart);
        runRunnerThreads();
    }

    private static void runRunnerThreads() {
        ExecutorService executor = Executors.newFixedThreadPool(MongoMonitorConfig.getMongoMonitorConfig().getShardSetClients().size());
        for (Map.Entry<String, MongoClient> client : MongoMonitorConfig.getMongoMonitorConfig().getShardSetClients().entrySet()) {
            Runnable worker = new OplogRunner(client, MongoMonitorConfig.getMongoMonitorConfig().getTimeDB());
            executor.execute(worker);
        }
        executor.shutdown();
    }
}
