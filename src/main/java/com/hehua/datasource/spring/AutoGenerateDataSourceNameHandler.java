/**
 * 
 */
package com.hehua.datasource.spring;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * 
 * @author SamChi <sam@afanda.com>
 * @Date 2012-8-16
 */
public class AutoGenerateDataSourceNameHandler extends NamespaceHandlerSupport {

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.xml.NamespaceHandler#init()
     */
    @Override
    public void init() {
        registerBeanDefinitionParser("auto-datasource", new AutoGenerateDataSourceParser());
    }
}