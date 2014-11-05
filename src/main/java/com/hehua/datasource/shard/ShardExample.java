/**
 * 
 */
package com.hehua.datasource.shard;

/**
 * 
 * @author zhouzhihua <zhihua@afanda.com>
 * @version 1.0 create at Oct 22, 2013 3:31:36 PM
 */
public class ShardExample {

    /**
     * @param args
     */
    public static void main(String[] args) {

        ShardJdbcTemplate readJdbcTemplate = new ShardJdbcTemplate("item_log", false);
        ShardJdbcTemplate writeJdbcTemplate = new ShardJdbcTemplate("item_log", true);
    }
}
