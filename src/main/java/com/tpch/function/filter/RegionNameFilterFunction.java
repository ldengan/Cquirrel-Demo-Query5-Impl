package com.tpch.function.filter;

import com.tpch.domain.model.Region;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;

public class RegionNameFilterFunction extends RichFilterFunction<Region> {
    @Override
    public boolean filter(Region region) {
        // region name = 'AMERICA'
        return StringUtils.isNotBlank(region.name)
                && region.name.equals(Region.RegionKey.AMERICA.name());
    }
}