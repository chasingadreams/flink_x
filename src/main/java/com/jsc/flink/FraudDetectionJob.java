package com.jsc.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @Description
 * @Date 2022/3/17 16:04
 * @Create by 葡萄
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactionDataStream = environment
                .addSource(new TransactionSource())
                .name("transaction");

        DataStream<Alert> alerts = transactionDataStream
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts.addSink(new AlertSink()).name("send-alerts");

        environment.execute("Fraud Detection");
    }
}
