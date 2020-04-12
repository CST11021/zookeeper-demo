package lock.test2;

import java.util.concurrent.TimeUnit;

public interface DistributedLock {

    /**
     * 获取锁，如果没有得到就等待
     */
    public void acquire() throws Exception;

    /**
     * 获取锁，直到超时
     *
     * @param time 超时时间
     * @param unit time参数的单位
     * @throws Exception
     * @return是否获取到锁
     */
    public boolean acquire(long time, TimeUnit unit) throws Exception;

    /**
     * 释放锁
     *
     * @throws Exception
     */
    public void release() throws Exception;
}