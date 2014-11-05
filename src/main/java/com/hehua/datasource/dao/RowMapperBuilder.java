package com.hehua.datasource.dao;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

/**
 * RowMapper自动构建器
 * 
 * @author SamChi <sam@afanda.com>
 * @Date 2012-8-17
 */
public final class RowMapperBuilder {

    private RowMapperBuilder() {

    }

    public static <T> RowMapper<T> buildRowMapper(final Class<T> clazz) {
        return new BeanPropertyRowMapper<T>(clazz) {};
    }

}
