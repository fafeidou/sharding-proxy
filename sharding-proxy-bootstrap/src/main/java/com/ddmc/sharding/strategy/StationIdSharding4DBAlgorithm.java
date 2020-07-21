package com.ddmc.sharding.strategy;

import com.ddmc.sharding.ShardingProperties;
import com.ddmc.sharding.strategy.StationIdShardingAlgorithm;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

/**
 * @author liangtonghui
 * @date 2020-06-16
 */
@Slf4j
public class StationIdSharding4DBAlgorithm extends StationIdShardingAlgorithm {

    private ShardingProperties shardingProperties = new ShardingProperties();

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Integer> shardingValue) {
        // 库路由匹配
        return doShardingByStationId4DB(shardingValue, shardingProperties);
    }

}
