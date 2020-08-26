package com.zhisheng.sql.ago.sql;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Objects;

/**
 * @author mxb
 * @since 2020-03-30
 */
public class StreamSQLDistinct {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // nc -l 9400
        //{"user":1,"name":"peter","productId":1,"amount":3,"type":"C"}
        //{"user":1,"name":"peter","productId":1,"amount":6,"type":"U"}
        //{"user":2,"name":"robert","productId":2,"amount":4,"type":"C"}
        //{"user":3,"name":"john","productId":3,"amount":2,"type":"C"}
        DataStream<Order> order = env.socketTextStream("127.0.0.1", 9400).map(new MapFunction<String, Order>() {
            @Override
            public Order map(String s) throws Exception {
                return JSONObject.parseObject(s, Order.class);
            }
        });

        // nc -l 9300
        //{"productId":1,"productName":"rubber","type":"C"}
        //{"productId":1,"productName":"rubber1","type":"U"}
        //{"productId":3,"productName":"apple","type":"C"}
        //{"productId":4,"productName":"pair","type":"C"}  无输出
        DataStream<Product> product=env.socketTextStream("127.0.0.1", 9300).map(new MapFunction<String, Product>() {
            @Override
            public Product map(String s) throws Exception {
                return JSONObject.parseObject(s,Product.class);
            }
        });

        //设定状态最大时间
        tEnv.getConfig().setIdleStateRetentionTime(Time.seconds(10), Time.minutes(6));
        tEnv.registerDataStream("OrderA", order, "user,name,productId,amount,type");
        tEnv.registerDataStream("Product", product, "productId, productName,type");

        Table result = tEnv.sqlQuery("select Distinct p.productName from OrderA o left join Product p on o.productId = p.productId");

        tEnv.toRetractStream(result, OrderProduct.class).filter(new FilterFunction<Tuple2<Boolean, OrderProduct>>() {
            @Override
            public boolean filter(Tuple2<Boolean, OrderProduct> booleanOrderProductTuple2) throws Exception {
                return booleanOrderProductTuple2.f0;
            }
        }).print();

        env.execute();
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {

        public Long user;

        public String name;

        public int productId;

        public int amount;

        public String type;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Product{
        public int productId;
        public String productName;
        public String type;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderProduct{

        public String name;
        public int amount;

        public String productName;
    }
}
