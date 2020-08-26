package com.zhisheng.sql.ago.sql;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Objects;

/**
 * left join 且 支流按顺序输出
 * @author mxb
 * @since 2020-03-30
 */
public class StreamSQLLeftJoinAsc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new RocksDBStateBackend("file:///Users/mac/Public/rockdb",true));
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // nc -l 9400
        //{"user":1,"name":"peter","productId":1,"amount":3}
        //{"user":4,"name":"peter1","productId":1,"amount":3}
        //{"user":2,"name":"robert","productId":2,"amount":4}
        //{"user":3,"name":"john","productId":3,"amount":2}
        DataStream<Order> order = env.socketTextStream("127.0.0.1", 9400).map(new MapFunction<String, Order>() {
            @Override
            public Order map(String s) throws Exception {
                return JSONObject.parseObject(s, Order.class);
            }
        });

        // nc -l 9300
        //{"productId":1,"productName":"C","createTime":"2020-06-22 14:11:23"}
        //{"productId":1,"productName":"U","createTime":"2020-06-22 14:12:23"}
        //{"productId":1,"productName":"D","createTime":"2020-06-22 14:11:55"}
        //{"productId":2,"productName":"rubber4","createTime":"2020-06-22 14:11:23"}
        //{"productId":3,"productName":"apple","createTime":"2020-06-22 14:14:23"}
        //{"productId":4,"productName":"apple","createTime":"2020-06-22 15:11:23"}  无输出
        DataStream<Product> product=env.socketTextStream("127.0.0.1", 9300).map(new MapFunction<String, Product>() {
            @Override
            public Product map(String s) throws Exception {
                return JSONObject.parseObject(s, Product.class);
            }
        });

        //设定状态最大时间
        tEnv.getConfig().setIdleStateRetentionTime(Time.seconds(10), Time.minutes(6));
        tEnv.registerDataStream("OrderA", order, "user,name,productId,amount");
        tEnv.registerDataStream("Product", product, "createTime,productId, productName");

        Table result = tEnv.sqlQuery("select o.*,p.productName,p.createTime from OrderA o left join Product p on o.productId = p.productId");

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
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Product{
        public String createTime;
        public int productId;
        public String productName;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Product product = (Product) o;
            return productId == product.productId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(productId);
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderProduct{
        public Long user;

        public String name;

        public int productId;

        public int amount;

        public String productName;

        public String createTime;
    }
}
