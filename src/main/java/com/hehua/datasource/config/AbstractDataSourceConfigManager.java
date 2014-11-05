package com.hehua.datasource.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.google.common.collect.Maps;
import com.hehua.commons.utils.ObjectMapperUtils;
import com.hehua.datasource.config.DataSourceMappingConfig.BizNameKey;
import com.hehua.datasource.utils.DataSourceWrapper;
import com.hehua.datasource.utils.ObjectHolder;

public abstract class AbstractDataSourceConfigManager implements DataSourceConfigManager {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    private final String DEFAULT_DATASOURCE_CONFIG_FILE = "/config/datasource_default_config.properties";

    /**
     * the global data structure of data source configuration
     */
    protected DataSourceMappingConfig config;

    /**
     * the default data source properties
     */
    protected final Properties defaultDataSourceProperties;

    protected AbstractDataSourceConfigManager() {
        boolean lazyInit = this.isLazyInit();
        this.defaultDataSourceProperties = this.getDefaultDataSourceProperties();
        this.config = this.initConfig(lazyInit);
    }

    @Override
    public DataSource getDataSource(String bizName, boolean write) {
        if (this.config == null) {
            throw new RuntimeException("the data source config not inited!");
        }
        DataSourceInfo dataSourceInfo = config.getConfigs().get(new BizNameKey(bizName, write));
        if (dataSourceInfo == null) {
            // try to get the opposite connection
            dataSourceInfo = config.getConfigs().get(new BizNameKey(bizName, !write));
            if (dataSourceInfo != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("cannot find datasource:" + bizName + "," + write
                            + ", try to use the opposite dataSource.");
                }
            } else {
                throw new RuntimeException("cannot find datasource:" + bizName + "@" + write
                        + ", please check the config!");
            }
        }
        DataSource dataSource = dataSourceInfo.getNode().getInnerObject();
        return dataSource;
    }

    @Override
    public DataSource getShardDataSource(String bizName, boolean write, int shardId) {
        return this.config.getShardConfigs().get(new BizNameKey(bizName, write))
                .getDataSource(shardId);
    }

    @Override
    public NamedParameterJdbcTemplate getShardJdbcTemplate(String bizName, boolean write,
            int shardId) {
        return this.config.getShardConfigs().get(new BizNameKey(bizName, write))
                .getJdbcTemplate(shardId);
    }

    @Override
    public String getTableName(String bizName, boolean write, int shardId) {
        return this.config.getShardConfigs().get(new BizNameKey(bizName, write))
                .getTableName(shardId);
    }

    @Override
    public Set<BizNameKey> getBizNames() {
        return this.config.getConfigs().keySet();
    }

    public Set<BizNameKey> getShardBizNames() {
        return this.config.getShardConfigs().keySet();
    }

    protected abstract String getConfigContent();

    private boolean isLazyInit() {
        return "ture".equals(System.getProperty("dataSourceLazyInit"));
    }

    private Properties getDefaultDataSourceProperties() {
        InputStream is = this.getClass().getResourceAsStream(DEFAULT_DATASOURCE_CONFIG_FILE);
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            logger.error("Ops!", e);
            throw new RuntimeException("load default datasource config error!", e);
        }
        return prop;
    }

    private DataSourceMappingConfig initConfig(boolean lazyInit) {
        String configContent = this.getConfigContent();
        if (StringUtils.isBlank(configContent)) {
            throw new RuntimeException("config content is empty!");
        }

        Map<BizNameKey, DataSourceInfo> result = Maps.newHashMap();
        Map<BizNameKey, ShardDataSourceInfo> shardResult = Maps.newHashMap();

        try (Scanner scanner = new Scanner(configContent)) {
            while (scanner.hasNextLine()) {
                String line = StringUtils.strip(scanner.nextLine());
                //跳过空行和注释,#为行注释
                if (StringUtils.isBlank(line) || line.charAt(0) == '#') {
                    continue;
                }

                //解析配置节点
                Map<String, String> node;
                try {
                    node = ObjectMapperUtils.fromJSON(line, Map.class, String.class, String.class);
                } catch (Exception e) {
                    logger.error("datasource resolve config error:" + line, e);
                    throw e;
                }

                DataSourceWrapper dataSourceWrapper = this.createDataSourceWrapper(lazyInit, node);

                final String bizName = node.get(DataSourcePropertyConstants.BIZ_NAME_KEY);
                final boolean write = DataSourcePropertyConstants.BOOLEAN_TRUE
                        .equalsIgnoreCase(node.get(DataSourcePropertyConstants.WRITE_KEY));
                final boolean shard = DataSourcePropertyConstants.BOOLEAN_TRUE
                        .equalsIgnoreCase(node.get(DataSourcePropertyConstants.SHARD_KEY));
                int weight = NumberUtils.toInt(node.get(DataSourcePropertyConstants.WEIGHT),
                        DataSourcePropertyConstants.DEFAULT_WEIGHT);
                String url = node.get(DataSourcePropertyConstants.CONFIG_KEY);

                BizNameKey key = new BizNameKey(bizName, write);

                if (!shard) {
                    // 非散表的
                    DataSourceInfo dataSourceInfo = result.get(key);
                    if (dataSourceInfo == null) {
                        dataSourceInfo = new DataSourceInfo();
                        result.put(key, dataSourceInfo);
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("found dataSource config, bizName:" + bizName + ", "
                                + (write ? "WRITE" : "READ") + ", weight:" + weight
                                + ", connection:" + url);
                    }
                    dataSourceInfo.putNode(dataSourceWrapper, weight);
                } else {
                    // 散表的
                    String tableName = node.get(DataSourcePropertyConstants.TABLE_NAME_KEY);
                    if (StringUtils.isBlank(tableName)) {
                        tableName = bizName;
                    }
                    ShardDataSourceInfo dataSourceInfo = shardResult.get(key);
                    if (dataSourceInfo == null) {
                        dataSourceInfo = new ShardDataSourceInfo(tableName);
                        shardResult.put(key, dataSourceInfo);
                    }

                    final int startPartition = NumberUtils.toInt(
                            node.get(DataSourcePropertyConstants.SHARD_BEGIN_KEY),
                            DataSourcePropertyConstants.DEFAULT_SHARD_START);

                    final int endPartition = NumberUtils.toInt(
                            node.get(DataSourcePropertyConstants.SHARD_END_KEY),
                            DataSourcePropertyConstants.DEFAULT_SHARD_END);

                    if (logger.isDebugEnabled()) {
                        logger.debug("found shardDataSource config, bizName:" + bizName + ", "
                                + (write ? "WRITE" : "READ") + ", weight:" + weight
                                + ", partition:[" + startPartition + "," + endPartition
                                + "], connection:" + url);
                    }

                    ObjectHolder<NamedParameterJdbcTemplate> jdbcTemplate = this
                            .createJdbcTemplate(dataSourceWrapper, lazyInit);
                    for (int i = startPartition; i <= endPartition; i++) {
                        dataSourceInfo.putNode(i, jdbcTemplate, dataSourceWrapper, weight);
                    }
                }

            }
        }

        return new DataSourceMappingConfig(result, shardResult);
    }

    private DataSourceWrapper createDataSourceWrapper(boolean lazyInit,
            final Map<String, String> node) {
        final DataSourceWrapper dataSourceWrapper;
        if (lazyInit) {
            dataSourceWrapper = new DataSourceWrapper() {

                private volatile DataSource dataSource;

                @Override
                public DataSource getInnerObject() {
                    if (dataSource == null) {
                        synchronized (this) {
                            if (dataSource == null) {
                                dataSource = buildDataSource(node);
                            }
                        }
                    }
                    return dataSource;
                }

                @Override
                public boolean inited() {
                    return this.dataSource != null;
                }

            };
        } else {
            dataSourceWrapper = new DataSourceWrapper() {

                private final DataSource dataSource = buildDataSource(node);

                @Override
                public DataSource getInnerObject() {
                    return this.dataSource;
                }

                @Override
                public boolean inited() {
                    return dataSource != null;
                }

            };
        }

        return dataSourceWrapper;
    }

    private DataSource buildDataSource(Map<String, String> node) {
        Properties properties = new Properties();
        for (Map.Entry<Object, Object> entry : this.defaultDataSourceProperties.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        properties.setProperty("username", node.get(DataSourcePropertyConstants.USERNAME_KEY));
        properties.setProperty("password", node.get(DataSourcePropertyConstants.PASSWORD_KEY));
        properties.setProperty("url", node.get(DataSourcePropertyConstants.CONFIG_KEY));
        try {
            //            return BasicDataSourceFactory.createDataSource(properties);
            return DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            logger.error("Ops!", e);
            throw new RuntimeException(e);
        }
    }

    private ObjectHolder<NamedParameterJdbcTemplate> createJdbcTemplate(
            final DataSourceWrapper dataSourceWrapper, boolean lazyInit) {
        final ObjectHolder<NamedParameterJdbcTemplate> objectHolder;
        if (lazyInit) {
            objectHolder = new ObjectHolder<NamedParameterJdbcTemplate>() {

                private volatile NamedParameterJdbcTemplate jdbcTemplate = null;

                @Override
                public NamedParameterJdbcTemplate getInnerObject() {
                    if (this.jdbcTemplate == null) {
                        synchronized (this) {
                            if (this.jdbcTemplate == null) {
                                this.jdbcTemplate = new NamedParameterJdbcTemplate(
                                        dataSourceWrapper.getInnerObject());
                            }
                        }
                    }
                    return this.jdbcTemplate;
                }

            };
        } else {
            objectHolder = new ObjectHolder<NamedParameterJdbcTemplate>() {

                private final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(
                        dataSourceWrapper.getInnerObject());

                @Override
                public NamedParameterJdbcTemplate getInnerObject() {
                    return this.jdbcTemplate;
                }

            };
        }

        return objectHolder;
    }
}
