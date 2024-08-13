package com.tpch.function.process;

import com.tpch.domain.model.Nation;
import com.tpch.domain.model.Region;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Input: Nation, Region
 * OutPut: update, nation.name, nation.nationKey
 */
public class NationJoinRegionFunction extends CoProcessFunction<Region, Nation, Tuple3<Boolean, String, Integer>> {
    private ValueState<Region> regionState;
    private ListState<Nation> nationListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        regionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("regionState", Region.class)
        );
        nationListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("nationState", Nation.class)
        );
    }

    @Override
    public void processElement1(Region region, CoProcessFunction<Region, Nation, Tuple3<Boolean, String, Integer>>.Context context, Collector<Tuple3<Boolean, String, Integer>> collector) throws Exception {
        Region oldRegion = regionState.value();

        if ((oldRegion == null && region.update) || (oldRegion != null && !region.update)) {
            regionState.update(region);
            for (Nation nation : nationListState.get()) {
                boolean update = region.update && nation.update;
                Tuple3<Boolean, String, Integer> tuple = Tuple3.of(update, nation.name, nation.nationKey);
                if (region.update || nation.update) {
                    collector.collect(tuple);
                }
            }
        }
    }

    @Override
    public void processElement2(Nation nation, CoProcessFunction<Region, Nation, Tuple3<Boolean, String, Integer>>.Context context, Collector<Tuple3<Boolean, String, Integer>> collector) throws Exception {
        boolean canAddToList = nation.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (Nation n : nationListState.get()) {
                if (n.update) {
                    insertCount += 1;
                } else {
                    deleteCount += 1;
                }
            }
            if (insertCount > deleteCount) {
                canAddToList = true;
            }
        }
        if (!canAddToList) {
            return;
        }
        nationListState.add(nation);
        Region region = regionState.value();
        if (region != null) {
            boolean update = region.update && nation.update;
            Tuple3<Boolean, String, Integer> tuple = Tuple3.of(update, nation.name, nation.nationKey);
            if (region.update || nation.update) {
                collector.collect(tuple);
            }
        }
    }
}