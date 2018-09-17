package com.whz.zookeeper.curator.barrier;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 分布式双栏栅Demo
 */
public class DistributedDoubleBarrierDemo {

    private static final String CONNECT_PATH = "localhost:2181";

    // Session 超时时间
    private static final int SESSION_TIME_OUT = 60000;

    // 连接超时
    private static final int CONNECT_TIME_OUT = 5000;

    public static void main(String[] args) {

        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {

                public void run() {
                    try {
                        // 1、设定重试策略
                        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 5);

                        // 2、获取工厂类
                        CuratorFramework cf = CuratorFrameworkFactory.builder().connectString(CONNECT_PATH).retryPolicy(retryPolicy)
                                .sessionTimeoutMs(SESSION_TIME_OUT).connectionTimeoutMs(CONNECT_TIME_OUT).build();

                        // 3、启动连接
                        cf.start();

                        //设置Barrier  5个同时准备的情况，这个是放在Thread 里面，就相当于每个线程都获取DistributedDoubleBarrier,然后更新操作里面的值
                        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(cf, "/dobuleBarrirer", 5);
                        System.out.println(Thread.currentThread().getName() + "\t同时等待");
                        barrier.enter();
                        System.out.println(Thread.currentThread().getName() + "\t同时执行任务");
                        barrier.leave();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }
}