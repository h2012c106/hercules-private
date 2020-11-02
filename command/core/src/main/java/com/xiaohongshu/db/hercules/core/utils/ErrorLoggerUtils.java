package com.xiaohongshu.db.hercules.core.utils;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ErrorLoggerUtils {

    private static final Log LOG = LogFactory.getLog(ErrorLoggerUtils.class);

    private static final int MAX_LOG_LINE = 64;

    private static final Map<String, ErrorPool> POOL_POOL = new ConcurrentHashMap<>();

    private static Function<String, ErrorPool> newPoolFunc() {
        // return key -> new PreciseDumbassErrorPool(MAX_LOG_LINE);
        return key -> new PreciseFastErrorPool(MAX_LOG_LINE);
    }

    /**
     *
     * @param seq 在总体数据集中的序号
     */
    public static void add(String errorTag, HerculesWritable row, int seq) {
        POOL_POOL.computeIfAbsent(errorTag, newPoolFunc()).add(row, seq);
    }

    public static void print(String errorTag) throws InterruptedException {
        print(errorTag, LOG);
    }

    public static void print(String errorTag, Log logger) throws InterruptedException {
        ErrorPool pool = POOL_POOL.get(errorTag);
        if (pool != null) {
            List<HerculesWritable> errorList = pool.done();
            String errorListStr = StringUtils.join(errorList, "\n");
            int thrownNum = pool.getThrownNum();
            String message;
            if (thrownNum == 0) {
                message = String.format("Show all of the %d error line(s) for error<%s>:\n%s", errorList.size(), errorTag, errorListStr);
            } else {
                message = String.format("Show %d error line(s) with uniform distribution of error appearance order for error<%s>:\n%s\nand other %d row(s)...", errorList.size(), errorTag, errorListStr, thrownNum);
            }
            logger.warn(message);
        }
    }

    private interface ErrorPool {
        public void add(HerculesWritable rowValue, int generalSeq);

        public List<HerculesWritable> done() throws InterruptedException;

        public int getThrownNum();
    }

    /**
     * 设最大允许日志行数为n，每次add复杂度为O(n)
     */
    private static class PreciseDumbassErrorPool implements ErrorPool {
        private static final int MISSION_LENGTH = 64;

        private final ExecutorService worker = Executors.newFixedThreadPool(1);
        private final BlockingQueue<Mission> missionQueue = new ArrayBlockingQueue<>(MISSION_LENGTH);
        private final LinkedList<Row> linkedList = new LinkedList<>();

        private final int maxLogLine;
        private int thrownNum = 0;

        private int thrownMissionNum = 0;

        public PreciseDumbassErrorPool(int maxLogLine) {
            if (maxLogLine < 0) {
                throw new RuntimeException("illegal error logger log line: " + maxLogLine);
            }
            this.maxLogLine = maxLogLine;
            this.worker.execute(new Runnable() {

                private void addRow(LinkedList<Row> linkedList, final Row row) {
                    // 此时里面只有一个元素的时候gap算不出来，不用算
                    if (linkedList.size() > 1) {
                        Row last = linkedList.pollLast();
                        Row lastLeft = linkedList.getLast();
                        // 计算出最后一个row的gap
                        last.setGap(row.getGeneralSeq() - lastLeft.getGeneralSeq());
                        linkedList.add(last);
                    }
                    linkedList.add(row);
                }

                private boolean pollRow(LinkedList<Row> linkedList) {
                    Optional<Row> minRowOptional = linkedList.stream().min(new Comparator<Row>() {
                        @Override
                        public int compare(Row o1, Row o2) {
                            return o1.compareTo(o2);
                        }
                    });
                    if (minRowOptional.isPresent() && minRowOptional.get().getGap() != null) {
                        Row minRow = minRowOptional.get();
                        int seq = linkedList.indexOf(minRow);
                        Row left = linkedList.get(seq - 1);
                        Row right = linkedList.get(seq + 1);
                        linkedList.remove(seq);
                        if (left.getGap() != null) {
                            int leftToTop = minRow.getGeneralSeq() - left.getGeneralSeq();
                            left.setGap(left.getGap() + minRow.getGap() - leftToTop);
                        }
                        if (right.getGap() != null) {
                            final int rightToTop = right.getGeneralSeq() - minRow.getGeneralSeq();
                            right.setGap(right.getGap() + minRow.getGap() - rightToTop);
                        }
                        return true;
                    } else {
                        return false;
                    }
                }

                @Override
                public void run() {
                    while (true) {
                        Mission mission;
                        try {
                            mission = missionQueue.take();
                        } catch (InterruptedException e) {
                            LOG.warn("Error logger worker's taking mission interrupted, " + e.getMessage());
                            break;
                        }
                        if (mission.isClose()) {
                            break;
                        }
                        Row row = mission.getRow();
                        if (maxLogLine == 0) {
                            thrownNum += 1;
                        } else if (maxLogLine == 1) {
                            // 只打开始的1个
                            if (linkedList.isEmpty()) {
                                linkedList.add(row);
                            } else {
                                thrownNum += 1;
                            }
                        } else {
                            // 先无脑塞进去，两种情况
                            // 1. 在maxLogLine内，说明还没塞多，那啥都不用做。
                            // 2. 超员了，那么取出当中gap最小的一个row，保持总数不超
                            // 这个策略简而言之就是每次把错误最密部分的元素丢掉，塞进新元素，
                            // 本策略保证所有错误中第一条与最后一条最后会被打出来，且是相对错误出现顺序最均匀的输出策略。
                            // 注：本策略仅在maxLogLine>=2时有效，因为只有此时，在超员时才能算出至少一个有意义的gap
                            addRow(linkedList, row);
                            if (linkedList.size() > maxLogLine) {
                                thrownNum += pollRow(linkedList) ? 1 : 0;
                            }
                        }
                    }
                }
            });
        }

        @Override
        public void add(HerculesWritable rowValue, int generalSeq) {
            if (!missionQueue.offer(Mission.row(new Row(rowValue, generalSeq)))) {
                ++thrownMissionNum;
            }
        }

        @Override
        public List<HerculesWritable> done() throws InterruptedException {
            missionQueue.put(Mission.close());
            worker.shutdown();
            worker.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            List<HerculesWritable> res = linkedList.stream().map(Row::getRowValue).collect(Collectors.toList());
            if (thrownMissionNum > 0) {
                LOG.info(String.format("Thrown %d mission(s) due to too long mission list.", thrownMissionNum));
            }
            return res;
        }

        @Override
        public int getThrownNum() {
            return thrownNum + thrownMissionNum;
        }
    }

    /**
     * 使用小顶堆进行排序，加速找最小，节点为双向链表，加速插入，设最大允许日志行数为n，每次add复杂度为O(log(n))
     */
    private static class PreciseFastErrorPool implements ErrorPool {
        private static final int MISSION_LENGTH = 64;

        private final ExecutorService worker = Executors.newFixedThreadPool(1);
        private final BlockingQueue<Mission> missionQueue = new ArrayBlockingQueue<>(MISSION_LENGTH);
        private final MyLinkedPriorityQueue<Row> linkedPriorityQueue = new MyLinkedPriorityQueue<>();

        private final int maxLogLine;
        private int thrownNum = 0;

        private int thrownMissionNum = 0;

        public PreciseFastErrorPool(int maxLogLine) {
            if (maxLogLine < 0) {
                throw new RuntimeException("illegal error logger log line: " + maxLogLine);
            }
            this.maxLogLine = maxLogLine;
            this.worker.execute(new Runnable() {

                private void addRow(MyLinkedPriorityQueue<Row> linkedPriorityQueue, final Row row) {
                    // 此时里面只有一个元素的时候gap算不出来，不用算
                    if (linkedPriorityQueue.size() > 1) {
                        LinkedNode<Row> last = linkedPriorityQueue.getLast();
                        final LinkedNode<Row> lastLeft = last.getLeft();
                        // 计算出最后一个row的gap
                        linkedPriorityQueue.rebalance(last, new Function<Row, Void>() {
                            @Override
                            public Void apply(Row last) {
                                last.setGap(row.getGeneralSeq() - lastLeft.getValue().getGeneralSeq());
                                return null;
                            }
                        });
                    }
                    linkedPriorityQueue.add(row);
                }

                private boolean pollRow(MyLinkedPriorityQueue<Row> linkedPriorityQueue) {
                    final LinkedNode<Row> top = linkedPriorityQueue.peek();
                    // 根为null由于null比任何gap都大说明根还是first或last，由于maxLogLine至少为2，即肯定还没超员，不用管
                    if (top.getValue().getGap() != null) {
                        linkedPriorityQueue.remove();
                        // 不需要讨论null，因为必不会动last、first
                        LinkedNode<Row> left = top.getLeft();
                        LinkedNode<Row> right = top.getRight();
                        // 先重新调整左右row的gap，若本就是first/last则不需要改
                        if (left.getValue().getGap() != null) {
                            final int leftToTop = top.getValue().getGeneralSeq() - left.getValue().getGeneralSeq();
                            linkedPriorityQueue.rebalance(left, new Function<Row, Void>() {
                                @Override
                                public Void apply(Row left) {
                                    // leftleft - left -   top    - right
                                    //          a      b (remove) c
                                    // left.newGap = a+b+c = a+b+b+c-b = left.gap + top.gap - b(leftToTop)
                                    left.setGap(left.getGap() + top.getValue().getGap() - leftToTop);
                                    return null;
                                }
                            });
                        }
                        if (right.getValue().getGap() != null) {
                            final int rightToTop = right.getValue().getGeneralSeq() - top.getValue().getGeneralSeq();
                            linkedPriorityQueue.rebalance(right, new Function<Row, Void>() {
                                @Override
                                public Void apply(Row right) {
                                    // 同left
                                    right.setGap(right.getGap() + top.getValue().getGap() - rightToTop);
                                    return null;
                                }
                            });
                        }
                        return true;
                    } else {
                        return false;
                    }
                }

                @Override
                public void run() {
                    while (true) {
                        Mission mission;
                        try {
                            mission = missionQueue.take();
                        } catch (InterruptedException e) {
                            LOG.warn("Error logger worker's taking mission interrupted, " + e.getMessage());
                            break;
                        }
                        if (mission.isClose()) {
                            break;
                        }
                        Row row = mission.getRow();
                        if (maxLogLine == 0) {
                            thrownNum += 1;
                        } else if (maxLogLine == 1) {
                            // 只打开始的1个
                            if (linkedPriorityQueue.isEmpty()) {
                                linkedPriorityQueue.add(row);
                            } else {
                                thrownNum += 1;
                            }
                        } else {
                            // 先无脑塞进去，两种情况
                            // 1. 在maxLogLine内，说明还没塞多，那啥都不用做。
                            // 2. 超员了，那么取出当中gap最小的一个row，保持总数不超
                            // 这个策略简而言之就是每次把错误最密部分的元素丢掉，塞进新元素，
                            // 本策略保证所有错误中第一条与最后一条最后会被打出来，且是相对错误出现顺序最均匀的输出策略。
                            // 注：本策略仅在maxLogLine>=2时有效，因为只有此时，在超员时才能算出至少一个有意义的gap
                            addRow(linkedPriorityQueue, row);
                            if (linkedPriorityQueue.size() > maxLogLine) {
                                thrownNum += pollRow(linkedPriorityQueue) ? 1 : 0;
                            }
                        }
                    }
                }
            });
        }

        @Override
        public void add(HerculesWritable rowValue, int generalSeq) {
            if (!missionQueue.offer(Mission.row(new Row(rowValue, generalSeq)))) {
                ++thrownMissionNum;
            }
        }

        @Override
        public List<HerculesWritable> done() throws InterruptedException {
            missionQueue.put(Mission.close());
            worker.shutdown();
            worker.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            List<HerculesWritable> res = new ArrayList<>(maxLogLine);
            for (LinkedNode<Row> tmpNode = linkedPriorityQueue.getFirst(); tmpNode != null; tmpNode = tmpNode.getRight()) {
                res.add(tmpNode.getValue().getRowValue());
            }
            if (thrownMissionNum > 0) {
                LOG.info(String.format("Thrown %d mission(s) due to too long mission list.", thrownMissionNum));
            }
            return res;
        }

        @Override
        public int getThrownNum() {
            return thrownNum + thrownMissionNum;
        }
    }

    private static class Mission {
        private Row row;
        private boolean close;

        private Mission() {
        }

        public static Mission row(Row row) {
            Mission res = new Mission();
            res.row = row;
            res.close = false;
            return res;
        }

        public static Mission close() {
            Mission res = new Mission();
            res.row = null;
            res.close = true;
            return res;
        }

        public Row getRow() {
            return row;
        }

        public boolean isClose() {
            return close;
        }
    }

    private static class Row implements Comparable<Row> {
        private HerculesWritable rowValue;
        /**
         * 左右两错误行之间的距离
         */
        private Integer gap;
        /**
         * 代表这是进来的第几行（包括正确的错误的全部数据在内）
         */
        private int generalSeq;

        public Row(HerculesWritable rowValue, int generalSeq) {
            this.rowValue = rowValue;
            this.gap = null;
            this.generalSeq = generalSeq;
        }

        public HerculesWritable getRowValue() {
            return rowValue;
        }

        public void setRowValue(HerculesWritable rowValue) {
            this.rowValue = rowValue;
        }

        public Integer getGap() {
            return gap;
        }

        public void setGap(Integer gap) {
            this.gap = gap;
        }

        public int getGeneralSeq() {
            return generalSeq;
        }

        public void setGeneralSeq(int generalSeq) {
            this.generalSeq = generalSeq;
        }

        @Override
        public int compareTo(Row o) {
            // gap为null的row，代表头或尾，不参与小顶堆竞选，永远挤在最后
            if (getGap() == null && o.getGap() == null) {
                return 0;
            } else if (getGap() == null) {
                return 1;
            } else if (o.getGap() == null) {
                return -1;
            } else {
                return getGap().compareTo(o.getGap());
            }
        }
    }

    private static class LinkedNode<E> {
        private final E value;
        private LinkedNode<E> left = null;
        private LinkedNode<E> right = null;

        public LinkedNode(@NonNull E value) {
            this.value = value;
        }

        public E getValue() {
            return value;
        }

        public LinkedNode<E> getLeft() {
            return left;
        }

        public void setLeft(LinkedNode<E> left) {
            this.left = left;
        }

        public LinkedNode<E> getRight() {
            return right;
        }

        public void setRight(LinkedNode<E> right) {
            this.right = right;
        }
    }

    /**
     * 非线程安全
     *
     * @param <T>
     */
    private static class MyLinkedPriorityQueue<T extends Comparable<T>> {
        private final PriorityQueue<LinkedNode<T>> delegate;

        private LinkedNode<T> first = null;
        private LinkedNode<T> last = null;

        private final Method removeEqMethod;

        @SneakyThrows
        public MyLinkedPriorityQueue() {
            this.delegate = new PriorityQueue<>(new Comparator<LinkedNode<T>>() {
                @Override
                public int compare(LinkedNode<T> o1, LinkedNode<T> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });
            this.removeEqMethod = delegate.getClass().getDeclaredMethod("removeEq", Object.class);
            this.removeEqMethod.setAccessible(true);
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public int size() {
            return delegate.size();
        }

        /**
         * add对小顶堆而言无顺序，对链表而言插尾
         *
         * @param item
         */
        public void add(T item) {
            LinkedNode<T> itemNode = new LinkedNode<>(item);
            if (isEmpty()) {
                first = itemNode;
            } else {
                itemNode.setLeft(last);
                last.setRight(itemNode);
            }
            last = itemNode;
            delegate.add(itemNode);
        }

        private LinkedNode<T> removeEq(PriorityQueue<LinkedNode<T>> queue, LinkedNode<T> obj) {
            boolean removed;
            try {
                removed = (Boolean) removeEqMethod.invoke(queue, obj);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            return removed ? obj : null;
        }

        public void rebalance(LinkedNode<T> obj, Function<T, Void> change) {
            obj = removeEq(delegate, obj);
            if (obj == null) {
                return;
            }
            change.apply(obj.getValue());
            delegate.add(obj);
        }

        /**
         * remove对小顶堆而言取顶，对链表而言可能是任何地方的remove
         *
         * @return
         */
        public LinkedNode<T> remove() {
            LinkedNode<T> itemNode = delegate.poll();
            if (itemNode == null) {
                return null;
            }
            LinkedNode<T> left = itemNode.getLeft();
            LinkedNode<T> right = itemNode.getRight();
            if (left != null && right != null) {
                left.setRight(right);
                right.setLeft(left);
            } else if (left == null && right != null) {
                right.setLeft(null);
            } else if (left != null) {
                left.setRight(null);
            }
            return itemNode;
        }

        public LinkedNode<T> peek() {
            return delegate.peek();
        }

        public LinkedNode<T> getFirst() {
            return first;
        }

        public LinkedNode<T> getLast() {
            return last;
        }
    }

}
