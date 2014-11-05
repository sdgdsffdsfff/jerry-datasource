/**
 * 
 */
package com.hehua.datasource.shard;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

import com.hehua.datasource.config.DataSourceConfigManager;
import com.hehua.datasource.config.DataSourceManager;

/**
 * @author w.vela <wangtianzhou@diandian.com>
 * 
 * @Date Apr 12, 2011 11:35:56 AM
 */
public class ShardJdbcTemplate {

    private final Log logger = LogFactory.getLog(getClass());

    public static final String SHARD_ID_KEY = "shard_id";

    private final String bizName;

    private final boolean write;

    /**
     * 获得表名
     * 
     * @param shardId
     * @return
     */
    public String getTableName(int shardId) {
        return DataSourceManager.getInstance().getTableName(bizName, write, shardId);
    }

    /**
     * 根据shardId来返回对应的NamedParameterJdbcTemplate(包含数据源)
     * 
     * @param shardId
     * @return
     */
    private NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(int shardId) {
        NamedParameterJdbcTemplate cachedTemplate = DataSourceManager.getInstance()
                .getShardJdbcTemplate(bizName, write, shardId);
        return cachedTemplate;
    }

    /**
     * @param dataSource
     */
    public ShardJdbcTemplate(String bizName, boolean write) {
        this.bizName = bizName;
        this.write = write;
    }

    /**
     * @param dataSource
     */
    public ShardJdbcTemplate(String bizName, boolean write,
            DataSourceConfigManager dataSourceConfigManager) {
        this.bizName = bizName;
        this.write = write;
    }

    /**
     * 查询
     * 
     * @param <T>
     * @param sql 表明请使用原始表明，比如abc_1请写abc，散表规则由配置决定
     * @param paramSource 请务必加入散表参数SHARD_ID_KEY
     * @param rowMapper
     * @return
     * @throws DataAccessException
     * @throws IllegalArgumentException
     */
    public <T> List<T> query(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper)
            throws DataAccessException, IllegalArgumentException {
        if (write) {
            throw new UnsupportedOperationException("你用的是写的template朋友，读不了啊！");
        }
        Integer shardId = (Integer) paramSource.getValue(SHARD_ID_KEY);
        if (shardId == null) {
            throw new IllegalArgumentException("请在paramSource里加入SHARD_ID_KEY参数用于散表配置");
        }
        return getNamedParameterJdbcTemplate(shardId).query(replaceTableName(sql, shardId),
                paramSource, rowMapper);
    }

    /**
     * 查询
     * 
     * @param sql 表明请使用原始表明，比如abc_1请写abc，散表规则由配置决定
     * @param paramSource 请务必加入散表参数SHARD_ID_KEY
     * @param rowCallbackHandler
     * @throws DataAccessException
     * @throws IllegalArgumentException
     */
    public void query(String sql, SqlParameterSource paramSource,
            RowCallbackHandler rowCallbackHandler) throws DataAccessException,
            IllegalArgumentException {
        if (write) {
            throw new UnsupportedOperationException("你用的是写的template朋友，读不了啊！");
        }
        Integer shardId = (Integer) paramSource.getValue(SHARD_ID_KEY);
        if (shardId == null) {
            throw new IllegalArgumentException("请在paramSource里加入SHARD_ID_KEY参数用于散表配置");
        }
        getNamedParameterJdbcTemplate(shardId).query(replaceTableName(sql, shardId), paramSource,
                rowCallbackHandler);
    }

    /**
     * 查询
     * 
     * @param <T>
     * @param sql 表明请使用原始表明，比如abc_1请写abc，散表规则由配置决定
     * @param paramSource 请务必加入散表参数SHARD_ID_KEY
     * @param rowMapper
     * @return
     * @throws DataAccessException
     * @throws IllegalArgumentException
     */
    public <T> T queryForObject(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper)
            throws DataAccessException, IllegalArgumentException {
        if (write) {
            throw new UnsupportedOperationException("你用的是写的template朋友，读不了啊！");
        }
        Integer shardId = (Integer) paramSource.getValue(SHARD_ID_KEY);
        if (shardId == null) {
            throw new IllegalArgumentException("请在paramSource里加入SHARD_ID_KEY参数用于散表配置");
        }
        return getNamedParameterJdbcTemplate(shardId).queryForObject(
                replaceTableName(sql, shardId), paramSource, rowMapper);
    }

    /**
     * 查询
     * 
     * @param <T>
     * @param sql 表明请使用原始表明，比如abc_1请写abc，散表规则由配置决定
     * @param paramSource 请务必加入散表参数SHARD_ID_KEY
     * @param rowMapper
     * @return
     * @throws DataAccessException
     * @throws IllegalArgumentException
     */
    public int queryForInt(String sql, SqlParameterSource paramSource) throws DataAccessException,
            IllegalArgumentException {
        if (write) {
            throw new UnsupportedOperationException("你用的是写的template朋友，读不了啊！");
        }
        Integer shardId = (Integer) paramSource.getValue(SHARD_ID_KEY);
        if (shardId == null) {
            throw new IllegalArgumentException("请在paramSource里加入SHARD_ID_KEY参数用于散表配置");
        }
        return getNamedParameterJdbcTemplate(shardId).queryForInt(replaceTableName(sql, shardId),
                paramSource);
    }

    /**
     * 更新记录
     * 
     * @param sql 表明请使用原始表明，比如abc_1请写abc，散表规则由配置决定
     * @param parameterSource 请务必加入散表参数SHARD_ID_KEY
     * @return
     * @throws DataAccessException
     * @throws IllegalArgumentException
     */
    public int update(String sql, SqlParameterSource parameterSource) throws DataAccessException,
            IllegalArgumentException {
        if (!write) {
            throw new UnsupportedOperationException("你用的是读的template朋友，写不了啊！");
        }
        Integer shardId = (Integer) parameterSource.getValue(SHARD_ID_KEY);
        return getNamedParameterJdbcTemplate(shardId).update(replaceTableName(sql, shardId),
                parameterSource);
    }

    /**
     * 更新记录
     * 
     * @param sql 表明请使用原始表明，比如abc_1请写abc，散表规则由配置决定
     * @param parameterSource 请务必加入散表参数SHARD_ID_KEY
     * @param keyHolder 自增长Id持有器
     * @return
     * @throws DataAccessException
     * @throws IllegalArgumentException
     */
    public int update(String sql, SqlParameterSource parameterSource, KeyHolder keyHolder)
            throws DataAccessException, IllegalArgumentException {
        if (!write) {
            throw new UnsupportedOperationException("你用的是读的template朋友，写不了啊！");
        }
        Integer shardId = (Integer) parameterSource.getValue(SHARD_ID_KEY);
        return getNamedParameterJdbcTemplate(shardId).update(replaceTableName(sql, shardId),
                parameterSource, keyHolder);
    }

    // cached map, 不用每次都用正则去匹配。
    //private final ConcurrentHashMap<TwoTuple<String, Integer>, String> cachedResultSql = new ConcurrentHashMap<TwoTuple<String, Integer>, String>();

    private final String replaceTableName(String sourceSql, int shardId)
            throws IllegalArgumentException {
        String resultSql;// = cachedResultSql.get(Tuple.tuple(sourceSql, shardId));

        for (Pattern pattern : REPLACE_TABLENAME_PATTERN) {
            Matcher matcher = pattern.matcher(sourceSql);
            if (matcher.find()) {
                String prefix = sourceSql.substring(0, matcher.start(1));
                String suffix = sourceSql.substring(matcher.end(1), sourceSql.length());
                resultSql = prefix + getTableName(shardId) + suffix;
                if (logger.isTraceEnabled()) {
                    logger.trace("transfer sql, source:" + sourceSql + ", target:" + resultSql);
                }
                //cachedResultSql.put(Tuple.tuple(sourceSql, shardId), resultSql);
                return resultSql;
            }
        }
        throw new IllegalArgumentException("无法找到散表的表名，请检查SQL，或者咨询w.vela");
    }

    // FIXME: 目前自动替换的表名insert into语法表明后面一定得有空格，不然就sb了，回头修复
    private static final Pattern[] REPLACE_TABLENAME_PATTERN = new Pattern[] {
            Pattern.compile("select.*?from\\s+([^\\s]*?)\\s.*where.*"), //
            Pattern.compile("update\\s+(.*?)\\s+set.*?"), //
            Pattern.compile("delete\\s+from\\s+(.*?)\\s+where.*"), //
            Pattern.compile("insert.*?into\\s+(.*?)\\s+.*"),//
            Pattern.compile("replace.*?into\\s+(.*?)\\s+.*") };
}
