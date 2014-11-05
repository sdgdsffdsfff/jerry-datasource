/**
 * 
 */
package com.hehua.datasource.utils;

import javax.sql.DataSource;

/**
 * DataSource 包装器
 * 
 * @author SamChi <sam@afanda.com>
 * @Date 2012-8-16
 */
public interface DataSourceWrapper extends ObjectHolder<DataSource> {

    public boolean inited();
}
