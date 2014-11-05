package com.hehua.datasource.config;

import java.util.Set;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.hehua.datasource.config.DataSourceMappingConfig.BizNameKey;

/**
 * 数据源配置管理
 * 
 * @author SamChi <sam@afanda.com>
 * @Date 2012-8-16
 */
public interface DataSourceConfigManager {

    public DataSource getDataSource(String bizName, boolean write);

    public DataSource getShardDataSource(String bizName, boolean write, int shardId);

    public NamedParameterJdbcTemplate getShardJdbcTemplate(String bizName, boolean write,
            int shardId);

    public String getTableName(String bizName, boolean write, int shardId);

    Set<BizNameKey> getBizNames();
}
