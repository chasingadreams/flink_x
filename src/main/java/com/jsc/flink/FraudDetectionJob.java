package com.jsc.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @Description 定义程序的数据流
 * @Date 2022/3/17 16:04
 * @Create by 葡萄
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        //1.设置执行环境，任务执行环境用于定义任务的属性、创建数据源以及最终启动任务的执行
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.创建数据源 数据源可由Apache Kafka、Rabbit MQ 或者 Apache Pulsar 接收数据
        //2.1 绑定到数据源上的name属性是为了快速定位异常发生的问题
        DataStream<Transaction> transactionDataStream = environment
                .addSource(new TransactionSource())
                .name("transaction");

        //3.定义任务的属性+任务内操作逻辑
        //3.1 transactionDataStream包含大量用户交易数据，需要划分至多个并发的task上进行处理
        //3.2 保证同一个账户的交易行为数据都要被同一个并发的task进行处理
        //3.4 同一个task处理同一个key的所有数据，可以使用DataStream::keyBy对流进行`分区`
        //3.5 process()函数对流绑定了操作，传入FraudDetector()
        DataStream<Alert> alerts = transactionDataStream
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        //4.定义预警任务的名称属性
        alerts.addSink(new AlertSink()).name("send-alerts");

        //5.最终任务的执行启动逻辑
        //5.1 Flink是懒加载,完全加载后之后上传至集群进行执行，调用StreamExecutionEnvironment#execute给任务传递一个任务名参数
        //5.2 传递参数名称用于后续任务的检索
        environment.execute("Fraud Detection");
    }
}
