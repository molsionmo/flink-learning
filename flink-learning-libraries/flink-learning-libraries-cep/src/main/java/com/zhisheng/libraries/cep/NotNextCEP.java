package com.zhisheng.libraries.cep;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

@Slf4j
public class NotNextCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Tuple3<Integer, String, String>> eventStream = env.fromElements(
//                Tuple3.of(1500, "login", "success"),
//                Tuple3.of(1500, "exit", "success"));

        // 1500,login,success
        // 1500,exit,success
        DataStream<Tuple3<Integer, String, String>> eventSocketStream =  env.socketTextStream("localhost", 9300).flatMap(new FlatMapFunction<String, Tuple3<Integer, String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<Integer, String, String>> collector) throws Exception {
                String[] split = s.split(",");
                Tuple3 tuple3 = new Tuple3<Integer, String, String>(Integer.valueOf(split[0]), split[1], split[2]);
                collector.collect(tuple3);
            }
        });

        // 登录成功10分钟后，没有投资
        Pattern<Tuple3<Integer,String,String>,?> pattern = Pattern.<Tuple3<Integer, String, String>>begin("begin")
                .where(new SimpleCondition<Tuple3<Integer,String,String>>() {
                    @Override
                    public boolean filter(Tuple3<Integer, String, String> s) throws Exception {
                        log.info("begin:{}", s);
                        return s.f1.equals("login") && s.f2.equals("success");
                    }
                });

        PatternStream<Tuple3<Integer, String, String>> patternStream = CEP.pattern(eventSocketStream.keyBy(x->x.f0), pattern);

        DataStream<String> alarmStream =
                patternStream.select(new PatternSelectFunction<Tuple3<Integer, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<Integer, String, String>>> map) throws Exception {
                        log.info("p: {}", map);
                        //String format = "ID %d has login failed 3 times in 5 seconds,but login success once in next 5 seconds.";
                        String msg = String.format("ID %d has login ,but exit without invest."
                                , map.values().iterator().next().get(0).f0);
                        return msg;
                    }
                });

        alarmStream.print();

        env.execute("cep test");
    }
}
