/**
 * 
 */
package com.hehua.datasource.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.google.common.collect.Sets;
import com.hehua.commons.WeightTreeInfo;
import com.hehua.datasource.utils.DataSourceWrapper;
import com.hehua.datasource.utils.ObjectHolder;

/**
 * @author w.vela <wangtianzhou@diandian.com>
 * 
 * @Date Apr 13, 2011 3:48:18 PM
 */
public class ShardDataSourceInfo implements Iterable<DataSourceWrapper> {

    private final Log logger = LogFactory.getLog(getClass());

    static class NamedJdbcTplSourceInfo extends
            WeightTreeInfo<ObjectHolder<NamedParameterJdbcTemplate>> {

    }

    private final Map<Integer, NamedJdbcTplSourceInfo> jdbcTemplates = new HashMap<Integer, ShardDataSourceInfo.NamedJdbcTplSourceInfo>();

    private final Map<Integer, DataSourceInfo> dataSources = new HashMap<Integer, DataSourceInfo>();

    private volatile int maxHashCount = 0;

    private final String tableName;

    /**
     * @param tableName
     */
    ShardDataSourceInfo(String tableName) {
        this.tableName = tableName;
    }

    public void putNode(int hash, ObjectHolder<NamedParameterJdbcTemplate> jdbcTemplate,
            DataSourceWrapper dataSource, int weight) {
        if (hash > maxHashCount) {
            maxHashCount = hash;
        }
        NamedJdbcTplSourceInfo jdbcTplSourceInfo = jdbcTemplates.get(hash);
        if (jdbcTplSourceInfo == null) {
            jdbcTplSourceInfo = new NamedJdbcTplSourceInfo();
            jdbcTemplates.put(hash, jdbcTplSourceInfo);
        }
        jdbcTplSourceInfo.putNode(jdbcTemplate, weight);

        DataSourceInfo dataSourceInfo = dataSources.get(hash);
        if (dataSourceInfo == null) {
            dataSourceInfo = new DataSourceInfo();
            dataSources.put(hash, dataSourceInfo);
        }
        dataSourceInfo.putNode(dataSource, weight);
    }

    public NamedParameterJdbcTemplate getJdbcTemplate(int shardId) {
        NamedJdbcTplSourceInfo namedJdbcTplSourceInfo = jdbcTemplates.get(shardId
                % (maxHashCount + 1));
        if (namedJdbcTplSourceInfo == null) {
            logger.error("there is no jdbcTemplate:" + shardId % (maxHashCount + 1));
            return null;
        }
        NamedParameterJdbcTemplate jdbcTemplate = namedJdbcTplSourceInfo.getNode().getInnerObject();
        return jdbcTemplate;
    }

    public DataSource getDataSource(int shardId) {
        DataSourceInfo dataSourceInfo = dataSources.get(shardId % (maxHashCount + 1));
        if (dataSourceInfo == null) {
            logger.error("there is no jdbcTemplate:" + shardId % (maxHashCount + 1));
            return null;
        }
        DataSource dataSource = dataSourceInfo.getNode().getInnerObject();
        return dataSource;
    }

    public String getTableName(int shardId) {
        int modCount = maxHashCount + 1;
        String shardTableName = tableName + "_" + shardId % modCount;
        if (logger.isTraceEnabled()) {
            logger.trace("shard_id:" + shardId + ", modCount:" + modCount + ", shardTableName:"
                    + shardTableName);
        }
        return shardTableName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<DataSourceWrapper> iterator() {
        Set<Iterator<DataSourceWrapper>> iterators = Sets.newHashSet();
        for (DataSourceInfo dataSource : dataSources.values()) {
            iterators.add(dataSource.iterator());
        }
        return new IteratorChain(iterators);
    }
}
