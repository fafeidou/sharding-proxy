package com.ddmc.sharding;

import com.google.common.collect.Sets;
import java.util.Set;
import lombok.Data;

/**
 * 分片属性配置
 */
@Data
public class ShardingProperties {

    /**
     * 逻辑库名.
     */
    private String databaseName = "fdc_do";

    /**
     * 物理库个数.
     */
    private Integer databaseNum = 8;

    /**
     * 逻辑表名列表：逗号分隔.
     */
    private String tableNames;

    /**
     * 每个物理库的物理表个数.
     */
    private Integer tableNum = 8;

    /**
     * 按照门店id分片规则的边界值，如果小于改值走旧规则；如果大于或者等于改值走新规则.
     */
    private Integer boundaryStationId = 1000;

    /**
     * 旧分片规则的数据库个数.
     */
    public static final int DATABASE_NUM_V1 = 4;

    /**
     * 测试门店集合.
     */
    private Set<Integer> testStationList = Sets.newConcurrentHashSet();

    /**
     * 测试门店数据库.
     */
    private Integer testDatabase = 1024;
}
