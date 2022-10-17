package com.example.internal;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.locks.Lock;

/**
 * 描述待提交的偏移量
 *
 * @ClassName CommitOffset
 * @Author lianxiaodong
 * @Date 2022年09月14日 10:10 AM
 */
@Getter
@Setter
public class CommitOffset {

    public static final byte INITIAL_FLAG = 0b0001;
    public static final byte PREPARE_COMMIT_FLAG = 0b0010;
    public static final byte COMMITTING_FLAG = 0b0100;
    public static final byte COMMITTED_FLAG = 0b1000;

    private TopicPartition topicPartition;

    private long offset;

    private long startTime;
    /**
     * 提交状态
     */
    private volatile byte commitState;

    private Throwable th = null;

    private volatile Lock commitLock;



    public CommitOffset(TopicPartition topicPartition, long offset) {
        this.topicPartition = topicPartition;
        // 由于只做提交含义，直接在此处进行额外的+1操作，避免后面的各种转换
        this.offset = offset + 1;
        this.commitState = INITIAL_FLAG;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * 是否初始化
     * @return
     */
    public boolean isInitial() {
        return (commitState & INITIAL_FLAG) > 0;
    }

    /**
     * 是否需要提交
     * @return
     */
    public boolean isPrepareCommitting() {
        return (commitState & PREPARE_COMMIT_FLAG) > 0;
    }

    /**
     * 是否提交中，如果有正在提交的偏移量，则不需要执行
     * @return
     */
    public boolean isCommitting() {
        return (COMMITTING_FLAG & commitState) > 0;
    }

    /**
     * 是否已经提交，包括失败提交
     * @return
     */
    public boolean isCommitted() {
        return (COMMITTED_FLAG & commitState) > 0;
    }

    public boolean isCommittable() {
        return commitState > INITIAL_FLAG;
    }


    /**
     * 是否已经成功提交
     *
     * @return
     */
    public boolean isCommittedSuccessfully() {
        return (COMMITTED_FLAG & commitState) > 0 && th == null;
    }

    /**
     * 准备提交
     */
    public void prepareCommit() {
        this.commitState = PREPARE_COMMIT_FLAG;
    }

    /**
     * 提交中
     */
    public void committing() {
        this.commitState = COMMITTING_FLAG;
    }

    /**
     * 提交结束
     * @param th 提交结果
     */
    public void committed(Throwable th) {
        this.commitState = COMMITTED_FLAG;
        this.th = th;
    }

    /**
     * 执行超时(可能存在死锁等问题)
     * @param executionTimeoutInMS
     * @return
     */
    public boolean maybeInLock(long executionTimeoutInMS) {
        return System.currentTimeMillis() - startTime > executionTimeoutInMS;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommitOffset that = (CommitOffset) o;

        return topicPartition.equals(that.topicPartition);
    }

    @Override
    public int hashCode() {
        return topicPartition.hashCode();
    }

    @Override
    public String toString() {
        return "CommitOffset{" +
                "topicPartition=" + topicPartition +
                ", offset=" + offset +
                ", startTime=" + startTime +
                ", commitState=" + commitState +
                ", th=" + th +
                '}';
    }
}
