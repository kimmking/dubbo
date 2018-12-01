package org.apache.dubbo.rpc.cluster.loadbalance;


import com.alibaba.fastjson.JSON;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Created by yizhenqiang&kimmking on 18/9/26.
 */
public class IntegrationLoadbalanceTest {

    /**
     * 假设该接口有10个可用的Invoker
     */

    private static final int[] INVOKER_WEIGHT_ARRAY = new int[]{100, 100, 200, 200, 300, 300, 400, 400, 500, 500};

    private static final int INVOKER_SIZE = INVOKER_WEIGHT_ARRAY.length;

    private static final String SERVICE_KEY = "com.test.Test.testMethod";

    private static final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    private static final ConcurrentMap<String, AtomicPositiveInteger> sequences1 = new ConcurrentHashMap<String, AtomicPositiveInteger>();
    private static final ConcurrentMap<String, AtomicPositiveInteger> weightSequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();


    static int ct = 0;

    public static void main(String[] args) {
        int times = 1000*1000;
        int[] selectArray = array(INVOKER_SIZE,0);
        long start = System.nanoTime();
        while (times-- > 0) {
            int select = newSelect1();
            selectArray[select]++;
        }

        System.out.println("最新dubbo的RoundRobinLoadBalance耗时：" + (System.nanoTime() - start) / 1000000);
        System.out.println("最新dubbo的RoundRobinLoadBalance流量分布：" + JSON.toJSONString(selectArray));

        System.out.println("count: "+ct);

        times = 1000000;
        selectArray = array(INVOKER_SIZE,0);
        start = System.nanoTime();
        while (times-- > 0) {
            int select = oldSelect();
            selectArray[select]++;
        }

        System.out.println("dubbo-2.5.3的RoundRobinLoadBalance耗时：" + (System.nanoTime() - start) / 1000000);
        System.out.println("dubbo-2.5.3的RoundRobinLoadBalance流量分布：" + JSON.toJSONString(selectArray));

        times = 1000000;
        selectArray = array(INVOKER_SIZE,0);
        start = System.nanoTime();
        while (times-- > 0) {
            int select = oldRandomSelect();
            selectArray[select]++;
        }

        System.out.println("dubbo-2.5.3的RandomLoadBalance耗时：" + (System.nanoTime() - start) / 1000000);
        System.out.println("dubbo-2.5.3的RandomLoadBalance流量分布：" + JSON.toJSONString(selectArray));

    }

    /**
     * 当前最新版本dubbo master分支中实现方式
     *
     * @return 选择的invoker的index
     */
    private static int currentSelect() {
        // 为了测试方便，key默认写死
        String key = SERVICE_KEY;
        // invoker默认是10个
        int length = INVOKER_SIZE; // Number of invokers

        int maxWeight = 0; // The maximum weight
        int minWeight = Integer.MAX_VALUE; // The minimum weight
        //final LinkedHashMap<Integer, IntegerWrapper> invokerToWeightMap = new LinkedHashMap<Integer, IntegerWrapper>();
        List<Integer> invokerW = new ArrayList<>();
        List<IntegerWrapper> invokerWIW = new ArrayList<>();
        int weightSum = 0;
        for (int i = 0; i < length; i++) {
            int weight = getWeight(i);
            maxWeight = Math.max(maxWeight, weight); // Choose the maximum weight
            minWeight = Math.min(minWeight, weight); // Choose the minimum weight
            if (weight > 0) {
                //invokerToWeightMap.put(i, new IntegerWrapper(weight));
                invokerW.add(i);
                invokerWIW.add(new IntegerWrapper(weight));
                weightSum += weight;
            }
        }
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }

        int currentSequence = sequence.getAndIncrement();
        if (maxWeight > 0 && minWeight < maxWeight) {
            int mod = currentSequence % weightSum;
            for (int i = 0; i < maxWeight; i++) {
                //for (Map.Entry<Integer, IntegerWrapper> each : invokerToWeightMap.entrySet()) {
                for (int j = 0; j < invokerW.size(); j++){
                    ct++;
                    final Integer k = invokerW.get(j);
                    final IntegerWrapper v = invokerWIW.get(j);
                    if (mod == 0 && v.getValue() > 0) {
                        return k;
                    }
                    if (v.getValue() > 0) {
                        v.decrement();
                        mod--;
                    }
                }
            }
        }
        // Round robin
        return currentSequence % length;
    }

    /**
     * 2.5.3版本的roundrobin方式
     *
     * @return
     */
    private static int oldSelect() {
        // 为了测试方便，key默认写死
        String key = SERVICE_KEY;
        // invoker默认是10个
        int length = INVOKER_SIZE; // Number of invokers

        List<Integer> invokers = new ArrayList<>();

        int maxWeight = 0; // 最大权重
        int minWeight = Integer.MAX_VALUE; // 最小权重
        for (int i = 0; i < length; i++) {
            int weight = getWeight(i);
            maxWeight = Math.max(maxWeight, weight); // 累计最大权重
            minWeight = Math.min(minWeight, weight); // 累计最小权重
        }
        if (maxWeight > 0 && minWeight < maxWeight) { // 权重不一样
            AtomicPositiveInteger weightSequence = weightSequences.get(key);
            if (weightSequence == null) {
                weightSequences.putIfAbsent(key, new AtomicPositiveInteger());
                weightSequence = weightSequences.get(key);
            }
            int currentWeight = weightSequence.getAndIncrement() % maxWeight;
            List<Integer> weightInvokers = new ArrayList<Integer>();
            for (int i = 0; i < INVOKER_SIZE; i++) { // 筛选权重大于当前权重基数的Invoker
                if (getWeight(i) > currentWeight) {
                    weightInvokers.add(i);
                }
            }
            int weightLength = weightInvokers.size();
            if (weightLength == 1) {
                return weightInvokers.get(0);
            } else if (weightLength > 1) {
                invokers = weightInvokers;
                length = invokers.size();
            }
        }
        AtomicPositiveInteger sequence = sequences1.get(key);
        if (sequence == null) {
            sequences1.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences1.get(key);
        }
        // 取模轮循
        return invokers.get(sequence.getAndIncrement() % length);
    }

    /**
     * 2.5.3版本的random方式
     *
     * @return
     */
    private static int oldRandomSelect() {
        // 为了测试方便，key默认写死
        String key = SERVICE_KEY;
        // invoker默认是10个
        int length = INVOKER_SIZE; // Number of invokers
        int totalWeight = 0; // 总权重

        boolean sameWeight = true; // 权重是否都一样
        for (int i = 0; i < length; i++) {
            int weight = getWeight(i);
            totalWeight += weight; // 累计总权重
            if (sameWeight && i > 0
                    && weight != getWeight(i - 1)) {
                sameWeight = false; // 计算所有权重是否一样
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // 如果权重不相同且权重大于0则按总权重数随机
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            // 并确定随机值落在哪个片断上
            for (int i = 0; i < length; i++) {
                offset -= getWeight(i);
                if (offset < 0) {
                    return i;
                }
            }
        }
        // 如果权重相同或权重为0则均等随机
        return ThreadLocalRandom.current().nextInt(length);
    }

    private static int getWeight(int invokerIndex) {
        return INVOKER_WEIGHT_ARRAY[invokerIndex];
    }

    private static int[] array(int size, int n){
        int[] arr = new int[size];
        Arrays.fill(arr,n);
        return arr;
    }

    private static final class IntegerWrapper {
        private int value;

        public IntegerWrapper(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void decrement() {
            this.value--;
        }
    }


    protected static int newSelect1() {
        // 为了测试方便，key默认写死
        String key = SERVICE_KEY;
        // invoker默认是10个
        int length = INVOKER_SIZE; // Number of invokers

        //List<Integer> invokers = new ArrayList<>();

        int maxWeight = 0; // 最大权重
        int minWeight = Integer.MAX_VALUE; // 最小权重

        final List<Integer> invokerToWeightList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            int weight = getWeight(i);
            maxWeight = Math.max(maxWeight, weight); // 累计最大权重
            minWeight = Math.min(minWeight, weight); // 累计最小权重
            if (weight > 0) {
                invokerToWeightList.add(i);
            }
        }
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }
        AtomicPositiveInteger indexSeq = sequences1.get(key);
        if (indexSeq == null) {
            sequences1.putIfAbsent(key, new AtomicPositiveInteger(-1));
            indexSeq = sequences1.get(key);
        }

        if (maxWeight > 0 && minWeight < maxWeight) {
            length = invokerToWeightList.size();
            while (true) {
                int index = indexSeq.incrementAndGet() % length;
                int currentWeight = sequence.get() % maxWeight;
                if (index == 0) {
                    currentWeight = sequence.incrementAndGet() % maxWeight;
                }
                if (getWeight(index) > currentWeight) {
                    return invokerToWeightList.get(index);
                }
            }
        }
        // Round robin
        return sequence.getAndIncrement() % length;
    }

    public static class AtomicPositiveInteger extends Number {

        private static final long serialVersionUID = -3038533876489105940L;

        private static final AtomicIntegerFieldUpdater<AtomicPositiveInteger> indexUpdater =
                AtomicIntegerFieldUpdater.newUpdater(AtomicPositiveInteger.class, "index");

        @SuppressWarnings("unused")
        private volatile int index = 0;

        public AtomicPositiveInteger() {
        }
        public AtomicPositiveInteger(int i) {
            this.index = i;
        }

        public final int getAndIncrement() {
            return indexUpdater.getAndIncrement(this) & Integer.MAX_VALUE;
        }

        public final int incrementAndGet() {
            return indexUpdater.incrementAndGet(this) & Integer.MAX_VALUE;
        }

        public final int get() {
            return indexUpdater.get(this) & Integer.MAX_VALUE;
        }

        @Override
        public byte byteValue() {
            return (byte) get();
        }

        @Override
        public short shortValue() {
            return (short) get();
        }

        @Override
        public int intValue() {
            return get();
        }

        @Override
        public long longValue() {
            return (long) get();
        }

        @Override
        public float floatValue() {
            return (float) get();
        }

        @Override
        public double doubleValue() {
            return (double) get();
        }
    }

}