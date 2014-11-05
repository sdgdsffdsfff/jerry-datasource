/**
 * 
 */
package com.hehua.datasource.config;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import com.hehua.framework.config.ZookeeperConfigManager;

/**
 * @author zhihua
 *
 */
public class ZookeeperResourceLoader implements ResourceLoader {

    private String configKey;

    /**
     * @param configKey
     */
    public ZookeeperResourceLoader(String configKey) {
        super();
        this.configKey = configKey;
    }

    @Override
    public ClassLoader getClassLoader() {
        return ZookeeperResourceLoader.class.getClassLoader();
    }

    @Override
    public Resource getResource(String arg0) {
        String config = ZookeeperConfigManager.getInstance().getString(configKey);
        if (config == null) {
            return null;
        }
        return new ByteArrayResource(config.getBytes());
    }

}
