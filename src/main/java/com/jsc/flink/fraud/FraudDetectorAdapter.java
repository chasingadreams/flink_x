package com.jsc.flink.fraud;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * @Description
 * @Date 2022/3/18 14:31
 * @Create by 葡萄
 */
public class FraudDetectorAdapter extends KeyedProcessFunction<Long, Transaction, Alert> {
    //1.多个事件之间存储信息需要使用到状态，使用KeyedProcessFunction来提供对状态和时间的细粒度操作
    //1.1 Flink提供一套支持状态容错的原语，最基础的状态类型为ValueState，
    //1.2 ValueState是一种key state，可以被用于key context提供的operator，能够紧随DataStream#keyBy之后被调用的operator
    //1.3 一个operator中的keyed state作用域默认是属于它所属的key的

    public static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;

    private static final double LARGE_AMOUNT = 500.00;

    private static final long ONE_MINUTE = 60 * 1000;

    //ValueState类似AtomicReference、AtomicLong，拥有三种方法update() value() clear()
    //ValueState实际上含三种状态 unset(null) | true | false
    private transient ValueState<Boolean> flagState;

    private transient ValueState<Long> timeState;

    @Override
    public void processElement(Transaction transaction, KeyedProcessFunction<Long, Transaction, Alert>.Context context, Collector<Alert> collector) throws Exception {

        //取出ValueState值
        Boolean lastTransactionWasSmall = flagState.value();

        //当程序启用或者调用过ValueState方法后ValueState#clear()方法时，ValueState#clear()方法会返回null值
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                //收集alert信息
                collector.collect(alert);
            }
            //标记清空
            //flagState.clear();
            cleanUpForInterpret(context);
        }

        //出现特定值则更新ValueState为true
        if (transaction.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            //注册定时器至context
            context.timerService().registerProcessingTimeTimer(timer);
            //处理时间是由本地时钟决定的
            timeState.update(timer);
        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {

        //ValueStateDescriptor包含Flink如何管理变量的元数据信息，使用前需要先使用open()注册
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN);

        //ValueState需要使用ValueStateDescriptor进行初始化
        //getRuntimeContext()获取当前类中的context
        flagState = getRuntimeContext().getState(flagDescriptor);

        //为定时器创建状态记录，专门用于记录定时器时间的状态
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<Long>("time-state", Types.LONG);

        timeState = getRuntimeContext().getState(timerDescriptor);
    }

    //定时器触发，回调onTime()方法
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, Transaction, Alert>.OnTimerContext ctx, Collector<Alert> out) throws Exception {
        timeState.clear();
        flagState.clear();
    }

    //未超时的情况下，需要通过手动清理定时器绑定
    private void cleanUpForInterpret(Context context) throws Exception {
        Long timer = timeState.value();
        context.timerService().deleteProcessingTimeTimer(timer);

        timeState.clear();
        flagState.clear();
    }

}
