package com.ddmc.sharding.strategy;

import com.ddmc.sharding.ShardingProperties;
import com.ddmc.sharding.util.ShardingDbUtil;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

/**
 * @author liangtonghui
 * @date 2020-06-16
 */
@Slf4j
public class StationIdShardingAlgorithm implements PreciseShardingAlgorithm<Integer> {

    /**
     * 实际数据库/表的模板.
     */
    protected static final String ACTUAL_TARGET_TEMPLATE = "%s_%d";

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Integer> shardingValue) {
        throw new RuntimeException("子类实现");
    }

    protected static String doShardingByStationId(Collection<String> availableTargetNames,
        PreciseShardingValue<Integer> shardingValue, ShardingProperties shardingProperties) {
        log.info("availableTargetNames:{},shardingValue:{},shardingProperties:{}",availableTargetNames,shardingValue,shardingProperties);
        // 获取可用节点列表的第一个
        String targetName = availableTargetNames.iterator().next();
        if (targetName.startsWith(shardingValue.getLogicTableName())) {
            // 表路由匹配
            return doShardingByStationId4Table(shardingValue, shardingProperties.getTableNum(), shardingProperties);
        } else {
            // 库路由匹配
            return doShardingByStationId4DB(shardingValue, shardingProperties);
        }
    }

    protected static String doShardingByStationId4Table(PreciseShardingValue<Integer> shardingValue, int tableNum,
        ShardingProperties shardingProperties) {
        // 表路由匹配
        if (shardingProperties.getTestStationList().contains(shardingValue.getValue())) {
            return shardingValue.getLogicTableName();
        }

        String targetTable;
        if (tableNum <= 1) {
            targetTable = shardingValue.getLogicTableName();
        } else {
            int tableIndex = ShardingDbUtil.getActualTableIndex(shardingValue.getValue(), tableNum);
            targetTable = String.format(ACTUAL_TARGET_TEMPLATE, shardingValue.getLogicTableName(), tableIndex);
        }
        log.debug("sharding table: {}", targetTable);
        return targetTable;
    }

    protected static String doShardingByStationId4DB(PreciseShardingValue<Integer> shardingValue,
        ShardingProperties shardingProperties) {
        log.info("shardingValue:{} , shardingProperties:{}",shardingValue,shardingProperties);
        if (shardingProperties.getTestStationList().contains(shardingValue.getValue())) {
            return String.format(ACTUAL_TARGET_TEMPLATE, shardingProperties.getDatabaseName(),
                shardingProperties.getTestDatabase());
        }

        Integer databaseIndex = getShardingDBIndex(shardingValue.getValue(), shardingProperties);
        String targetDatabase = String
            .format(ACTUAL_TARGET_TEMPLATE, shardingProperties.getDatabaseName(), databaseIndex);
        log.debug("sharding database: {}", targetDatabase);
        return targetDatabase;
    }

    /**
     * 根据门店id和门店边界值，获取DB的分片值
     *
     * @param stationId 门店id
     * @param shardingProperties 分片属性
     * @return DB的分片值
     */
    public static Integer getShardingDBIndex(Integer stationId, ShardingProperties shardingProperties) {
        if (shardingProperties.getTestStationList().contains(stationId)) {
            return shardingProperties.getTestDatabase();
        }
        Integer boundaryStationId = shardingProperties.getBoundaryStationId();
        return ShardingDbUtil.getShardingDBIndexByStationId(stationId, boundaryStationId);
    }

}
