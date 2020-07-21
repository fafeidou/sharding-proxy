package com.ddmc.sharding.strategy;

import com.ddmc.sharding.ShardingProperties;
import com.ddmc.sharding.strategy.StationIdShardingAlgorithm;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

/**
 * @author liangtonghui
 * @date 2019-10-25 11:56
 */
@Slf4j
public class DoPreciseShardingAlgorithm extends StationIdShardingAlgorithm {

    private ShardingProperties shardingProperties = new ShardingProperties();

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Integer> shardingValue) {
        return doShardingByStationId(availableTargetNames, shardingValue, shardingProperties);
    }
}
