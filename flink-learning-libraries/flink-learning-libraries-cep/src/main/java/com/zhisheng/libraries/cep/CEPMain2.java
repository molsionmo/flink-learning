package com.zhisheng.libraries.cep;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.util.List;
import java.util.Map;
@Slf4j
public class CEPMain2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Integer, String, String>> eventStream = env.fromElements(
                Tuple3.of(1500, "login", "fail"),
                Tuple3.of(1500, "exit", "success"));

        Pattern<Tuple3<Integer, String, String>, ?> loginFail =
                Pattern.<Tuple3<Integer, String, String>>begin("begin")
                        .where(new SimpleCondition<Tuple3<Integer, String, String>>() {
                            @Override
                            public boolean filter(Tuple3<Integer, String, String> s) throws Exception {
                                log.info("begin: {}", s);
                                return s.f1.equalsIgnoreCase("login");
                            }
                        })
                        .notFollowedBy("notInvest").where(new SimpleCondition<Tuple3<Integer, String, String>>(){
                            @Override
                            public boolean filter(Tuple3<Integer, String, String> s) throws Exception {
                                log.info("not invest: {}", s);
                                return s.f1.equalsIgnoreCase("invest");
                            }
                        })
                        .next("exit").where(new SimpleCondition<Tuple3<Integer, String, String>>() {
                            @Override
                            public boolean filter(Tuple3<Integer, String, String> tuple3) throws Exception {
                                log.info("exit: {}", tuple3);
                                return tuple3.f1.equals("exit");
                        }});

        PatternStream<Tuple3<Integer, String, String>> patternStream =
                CEP.pattern(eventStream.keyBy(x -> x.f0), loginFail);
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

        env.execute("cep event test");
    }
}
