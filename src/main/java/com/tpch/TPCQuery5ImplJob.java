package com.tpch;

import com.tpch.decode.DataSourceReader;
import com.tpch.domain.model.*;
import com.tpch.function.filter.OrderDateFilterFunction;
import com.tpch.function.process.*;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * --Q5
 * select
 *     n_name,
 *     sum(l_extendedprice * (1 - l_discount)) as revenue
 * from
 *     customer,
 *     orders,
 *     lineitem,
 *     supplier,
 *     nation,
 *     region
 * where
 *     c_custkey = o_custkey
 *     and l_orderkey = o_orderkey
 // *     and l_suppkey = s_suppkey
 *     and c_nationkey = s_nationkey
 *     and s_nationkey = n_nationkey
 *     and n_regionkey = r_regionkey
 // *     and r_name = 'ASIA' //filter
 *     and o_orderdate >= date '1994-01-01'
 *     and o_orderdate < date '1994-01-01' + interval '1' year
 * group by
 *     n_name
 * order by
 *     revenue desc;
 */
public class TPCQuery5ImplJob
{
    private static final OutputTag<Customer> customerOutputTag = new OutputTag<Customer>("customer"){};
    private static final OutputTag<Orders> ordersOutputTag = new OutputTag<Orders>("orders"){};
    private static final OutputTag<LineItem> lineItemOutputTag = new OutputTag<LineItem>("lineitem"){};
    private static final OutputTag<Supplier> supplierOutputTag = new OutputTag<Supplier>("supplier"){};
    private static final OutputTag<Nation> nationOutputTag = new OutputTag<Nation>("nation"){};
    private static final OutputTag<Region> regionOutputTag = new OutputTag<Region>("region"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //read datasource
        System.out.println("====== Start reading datasource......");
        DataStreamSource<String> rawDataStream = env.addSource(new DataSourceReader());
        System.out.println("====== Finished reading datasource......");
        // Get side output
        SingleOutputStreamOperator<String> processStream = rawDataStream
                .process(new OutPutTagSerializer());
        DataStream<Customer> customerDataStream = processStream.getSideOutput(customerOutputTag);
        DataStream<Orders> ordersDataStream = processStream.getSideOutput(ordersOutputTag);
        DataStream<LineItem> lineItemDataStream = processStream.getSideOutput(lineItemOutputTag);
        DataStream<Supplier> supplierDataStream = processStream.getSideOutput(supplierOutputTag);
        DataStream<Nation> nationDataStream = processStream.getSideOutput(nationOutputTag);
        DataStream<Region> regionDataStream = processStream.getSideOutput(regionOutputTag);

        // c_custkey = o_custkey and l_orderkey = o_orderkey
        SingleOutputStreamOperator<Tuple5<Boolean, Integer, Double, Double, Integer>> customJoinOrderJoinLineItemStream = customerDataStream
                .connect(
                        ordersDataStream
                                .filter(new OrderDateFilterFunction())
                )
                .keyBy(customer -> customer.custKey, orders -> orders.custKey)
                .process(new CustomerJoinOrderFunction()) // output: update, customer.custKey, customer.nationKey, orders.orderKey
                .connect(lineItemDataStream)
                .keyBy(order -> order.f3, lineItem -> lineItem.orderKey) // l_orderkey = o_orderkey
                .process(new CustomerAndOrderJoinLineItemFunction()); // output:update, customer.nationKey, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey
//        System.out.println("====== generate customJoinOrderJoinLineItem stream =====");
        customJoinOrderJoinLineItemStream.print();
        String outputPath = "/Users/ldengan/Desktop/ip/Cquirrel-Query5-Impl/data/running/output_data.csv";
        SingleOutputStreamOperator<Tuple4<Boolean, String, Integer, Integer>> supplierJoinNationAndRegionStream =
                regionDataStream
//                        .filter(new RegionNameFilterFunction())
                        .connect(nationDataStream)
                        .keyBy(r -> r.regionKey, n -> n.regionKey)
                        .process(new NationJoinRegionFunction()) // n_regionkey = r_regionkey
                        .connect(supplierDataStream)
                        .keyBy(t1 -> t1.f2, supplier -> supplier.nationKey)
                        .process(new SupplierJoinNationAndRegionFunction()); // s_nationkey = n_nationkey
//        System.out.println("====== generate supplierJoinNationAndRegion stream =====");
//        supplierJoinNationAndRegionStream.print();
//        System.out.println("====== Join All Stream =====");
//        DataStream<Tuple3<String, Double, Integer>> result =
        supplierJoinNationAndRegionStream
                .connect(customJoinOrderJoinLineItemStream)
                .keyBy(t1 -> t1.f2, t2 -> t2.f1)
                .process(new JoinAllFunction()) // c_nationkey = s_nationkey, 这里记得保存l_suppkey
//                .filter(t->t.f5.equals(t.f2)) // l_suppkey = s_suppkey
                .keyBy(t -> t.f1)
                .process(new CalculateNationRevenueFunction())// final statistical tables
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        //输出结果
//        result.keyBy(t -> t.f0) // 按nation name分区
//            .map(new RevenueStatisticalFormatMap())
//            .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    public static class OutPutTagSerializer extends ProcessFunction<String, String> {
        @Override
        public void processElement(String line, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
            String[] elements = line.split("\\|");
            String tag = elements[1];

            switch (tag) {
                case "customer":
                    context.output(customerOutputTag, new Customer(elements));
                    break;
                case "orders":
                    context.output(ordersOutputTag, new Orders(elements));
                    break;
                case "lineitem":
                    context.output(lineItemOutputTag, new LineItem(elements));
                    break;
                case "supplier":
                    context.output(supplierOutputTag, new Supplier(elements));
                    break;
                case "nation":
                    context.output(nationOutputTag, new Nation(elements));
                    break;
                case "region":
                    context.output(regionOutputTag, new Region(elements));
                    break;
                default:
                    break;
            }
        }
    }
}
