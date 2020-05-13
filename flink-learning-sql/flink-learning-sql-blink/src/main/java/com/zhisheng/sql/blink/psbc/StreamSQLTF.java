package com.zhisheng.sql.blink.psbc;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.List;

/**
 * 电信欺诈
 * @author mxb
 * @since 2020-03-30
 */
public class StreamSQLTF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        // nc -l 9400
        //{"call_details":[{"callType":"来电", "callStartTime":"2020-03-28 09:00:00", "callEndTime":"2020-03-28 09:00:5"},{"callType":"来电", "callStartTime":"2020-03-29 09:00:00", "callEndTime":"2020-03-29 09:00:10"}],"applyDateTime":"2020-03-29 10:00:00","caseNo":"123"}
        DataStream<SENSORS_CALL_DETAIL_ALL> sensors_call = env.socketTextStream("127.0.0.1", 9400).flatMap(new FlatMapFunction<String, SENSORS_CALL_DETAIL_ALL>() {
            @Override
            public void flatMap(String s, Collector<SENSORS_CALL_DETAIL_ALL> collector) throws Exception {
                SENSORS_CALL_DETAIL sensors_call_detail = JSONObject.parseObject(s, SENSORS_CALL_DETAIL.class);

                sensors_call_detail.getCall_details().forEach(call_detail -> {
                    SENSORS_CALL_DETAIL_ALL sensors_call_detail_all = new SENSORS_CALL_DETAIL_ALL(sensors_call_detail, call_detail);
                    collector.collect(sensors_call_detail_all);
                });
            }
        }).assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator());

        // nc -l 9300
        // {"caseNo":"123","submitDate":"2020-03-29 10:00:30"}
        DataStream<SENSORS_OPERATION> sensors_operation = env.socketTextStream("127.0.0.1", 9300).map(new MapFunction<String, SENSORS_OPERATION>() {
            @Override
            public SENSORS_OPERATION map(String s) throws Exception {
                return JSONObject.parseObject(s,SENSORS_OPERATION.class);
            }
        });

        //设定状态最大时间
        tEnv.getConfig().setIdleStateRetentionTime(Time.seconds(10), Time.minutes(6));

        // 设定applyDateTime为rowTime  https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/table/streaming/time_attributes.html
        tEnv.registerDataStream("sensors_call", sensors_call, "callType,callStartTime,callEndTime,caseNo,applyDateTime,systemTime.rowtime");
        tEnv.registerDataStream("sensors_operation", sensors_operation, "caseNo,submitDate");

        String sensorsCallView = "sensorsCallView";
        String view = "select caseNo,callType,callStartTime,callEndTime,cast(applyDateTime as VARCHAR) as applyDateTime,systemTime," +
                "cast(TIMESTAMPDIFF(SECOND, TO_TIMESTAMP(callStartTime), TO_TIMESTAMP(callEndTime)) as VARCHAR) as duration from sensors_call where callType='来电'";

        Table viewTable = tEnv.sqlQuery(view);
        tEnv.registerTable(sensorsCallView, viewTable);

        String callDurationViewName = "callDurationView";
        // caseNo最大通话时长
        Table callDurationView = tEnv.sqlQuery("select applyDateTime,caseNo,max(DISTINCT duration) as maxDuration from sensorsCallView group by TUMBLE(systemTime, INTERVAL '0.1' SECOND),caseNo,applyDateTime");
        tEnv.registerTable(callDurationViewName, callDurationView);
        tEnv.toRetractStream(callDurationView, GROUP_BY_CALL_ALONE_SINK.class).print();

        Table applyTimeTable = tEnv.sqlQuery("select cast(TIMESTAMPDIFF(SECOND, TO_TIMESTAMP(v.applyDateTime), TO_TIMESTAMP(o.submitDate)) as VARCHAR) as applyTime,v.caseNo,v.maxDuration," +
                "cast(v.maxDuration as DOUBLE) / TIMESTAMPDIFF(SECOND, TO_TIMESTAMP(v.applyDateTime), TO_TIMESTAMP(o.submitDate))  as percentage " +
                "from callDurationView v inner join sensors_operation o on v.caseNo = o.caseNo");
        tEnv.registerTable("applyTimeTable", applyTimeTable);

        tEnv.toRetractStream(tEnv.sqlQuery("select applyTime,caseNo,maxDuration,cast(ROUND(percentage,4) as VARCHAR) as percentage from applyTimeTable"), SENSORS_CALL_SINK.class).print();
        env.execute();
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SENSORS_CALL_SINK{
        //申请时长
        public String applyTime;
        //通话时长
        public String maxDuration;
        public String caseNo;
        //占比
        public String percentage;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static  class CALL_ALONE_SINK{
        public String caseNo;
        public String callType;
        public String callStartTime;
        public String callEndTime;
        public String applyDateTime;
        public String duration;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class GROUP_BY_CALL_ALONE_SINK {
        public String applyDateTime;
        public String maxDuration;
        public String caseNo;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SENSORS_CALL_DETAIL_ALL{
        public SENSORS_CALL_DETAIL_ALL(SENSORS_CALL_DETAIL sensors_call_detail, CALL_DETAIL call_detail){
            this.caseNo = sensors_call_detail.caseNo;
            this.applyDateTime = sensors_call_detail.applyDateTime;

            this.callType = call_detail.callType;
            this.callStartTime = call_detail.callStartTime;
            this.callEndTime = call_detail.callEndTime;
        }

        // SENSORS_CALL_DETAIL 平铺后的完整体
        public String callType;
        public String callStartTime;
        public String callEndTime;
        public String caseNo;
        public Timestamp applyDateTime;
        public Timestamp systemTime;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SENSORS_OPERATION{
        public String caseNo;
        public String submitDate;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SENSORS_CALL_DETAIL{
        public List<CALL_DETAIL> call_details;
        public String caseNo;
        public Timestamp applyDateTime;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CALL_DETAIL{
        public String callType;
        public String callStartTime;
        public String callEndTime;
    }

    public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<SENSORS_CALL_DETAIL_ALL> {

        private final long maxTimeLag = 60_000; // 60 seconds

        @Override
        public long extractTimestamp(SENSORS_CALL_DETAIL_ALL element, long previousElementTimestamp) {
            return System.currentTimeMillis();
//            return element.getApplyDateTime().getTime();
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
}
