package com.zhisheng.libraries.cep.office;

import com.zhisheng.libraries.cep.model.Event;
import com.zhisheng.libraries.cep.model.SubEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
 import java.util.Map;

 /**
 * 官方样例
 */
@Slf4j
public class CEPOfficeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.fromElements(
                new Event(10, "name10"),
                new Event(42, "name42"),
                new SubEvent(43, "subEventName43",11),
                new Event(44, "end"),
                new Event(45, "end")
                );

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                log.info("start:{}", event);
                return event.getId() == 42;
            }
        }).next("middle").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
            @Override
            public boolean filter(SubEvent subEvent) throws Exception {
                log.info("middle:{}", subEvent);
                return subEvent.getVolume() >= 10;
            }
        }).followedBy("end").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                log.info("end:{}", event);
                return event.getName().equals("end");
            }
        });

        PatternStream<Event> patternStream = CEP.pattern(input, pattern);
        DataStream<String> select = patternStream.select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> pattern) throws Exception {
                log.info("select result : {}", pattern);
                return pattern.toString();
            }
        });

        select.print();
        env.execute("flink cep official example");
    }
}
