package com.hehua.datasource.config;

/**
 * 数据源属性静态字段
 * 
 * @author sam
 * 
 */
interface DataSourcePropertyConstants {

    static final String WEIGHT = "weight";

    static final String BIZ_NAME_KEY = "bizName";

    static final String WRITE_KEY = "write";

    static final String BOOLEAN_TRUE = "true";

    static final String CONFIG_KEY = "url";

    static final String USERNAME_KEY = "user";

    static final String PASSWORD_KEY = "pass";

    static final String SHARD_KEY = "shard";

    static final String TEST_ON_BORROW_KEY = "testFraq";

    static final String SHARD_BEGIN_KEY = "shardBegin";

    static final String SHARD_END_KEY = "shardEnd";

    static final String TABLE_NAME_KEY = "tableName";

    static final int DEFAULT_WEIGHT = 10;

    static final int DEFAULT_SHARD_START = 0;

    static final int DEFAULT_SHARD_END = 99;

}
