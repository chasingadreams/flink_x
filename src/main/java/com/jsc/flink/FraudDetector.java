package com.jsc.flink;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * @Description
 * @Date 2022/3/17 16:34
 * @Create by 葡萄
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;

    private static final double LARGE_AMOUNT = 500.00;

    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(Transaction transaction, KeyedProcessFunction<Long, Transaction, Alert>.Context context, Collector<Alert> collector) throws Exception {

        Alert alert = new Alert();

        alert.setId(transaction.getAccountId());

        collector.collect(alert);

    }

}