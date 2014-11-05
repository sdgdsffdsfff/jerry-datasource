/**
 * 
 */
package com.hehua.datasource.config;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.iterators.IteratorChain;

import com.hehua.datasource.utils.DataSourceWrapper;
import com.google.common.collect.Sets;

/**
 * @author w.vela <wangtianzhou@diandian.com>
 * 
 * @Date Apr 8, 2011 1:23:47 AM
 */
public class DataSourceMappingConfig implements Iterable<DataSourceWrapper> {

    public static final class BizNameKey {

        private final String bizName;

        private final boolean write;

        /**
         * @return the bizName
         */
        public String getBizName() {
            return bizName;
        }

        /**
         * @return the write
         */
        public boolean isWrite() {
            return write;
        }

        /**
         * @param bizName
         * @param write
         */
        public BizNameKey(String bizName, boolean write) {
            super();
            this.bizName = bizName;
            this.write = write;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((bizName == null) ? 0 : bizName.hashCode());
            result = prime * result + (write ? 1231 : 1237);
            return result;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            BizNameKey other = (BizNameKey) obj;
            if (bizName == null) {
                if (other.bizName != null) return false;
            } else if (!bizName.equals(other.bizName)) return false;
            if (write != other.write) return false;
            return true;
        }

    }

    private final Map<BizNameKey, DataSourceInfo> configs;

    private final Map<BizNameKey, ShardDataSourceInfo> shardConfigs;

    /**
     * @param configs
     */
    public DataSourceMappingConfig(Map<BizNameKey, DataSourceInfo> configs,
            Map<BizNameKey, ShardDataSourceInfo> shardConfigs) {
        this.configs = configs;
        this.shardConfigs = shardConfigs;
    }

    /**
     * @return the configs
     */
    public Map<BizNameKey, DataSourceInfo> getConfigs() {
        return configs;
    }

    /**
     * @return the shardConfigs
     */
    public Map<BizNameKey, ShardDataSourceInfo> getShardConfigs() {
        return shardConfigs;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<DataSourceWrapper> iterator() {
        Set<Iterator<DataSourceWrapper>> allDataSourceIterator = Sets.newHashSet();
        for (DataSourceInfo config : configs.values()) {
            allDataSourceIterator.add(config.iterator());
        }
        for (ShardDataSourceInfo config : shardConfigs.values()) {
            allDataSourceIterator.add(config.iterator());
        }
        return new IteratorChain(allDataSourceIterator);
    }
}
