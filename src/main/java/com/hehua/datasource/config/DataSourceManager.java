package com.hehua.datasource.config;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import com.hehua.commons.resources.CompositeResourceLoader;
import com.hehua.commons.resources.SystemPropertyResourceLoader;

public class DataSourceManager extends AbstractDataSourceConfigManager {

    private static final String CONFIG_FILE_KEY = "datasourceConfigFile";

    public static final String DATASOURCE_CONF_KEY = "config/nodes.conf";

    private static ResourceLoader resourceLoader = new CompositeResourceLoader(
            new ResourceLoader[] { new SystemPropertyResourceLoader(CONFIG_FILE_KEY),
                    new ZookeeperResourceLoader("datasource"), new DefaultResourceLoader() });

    public void load() {
    }

    public ResourceLoader getResourceLoader() {
        return resourceLoader;
    }

    public static void setResourceLoader(ResourceLoader overrideResourceLoader) {
        resourceLoader = overrideResourceLoader;
    }

    private static DataSourceManager instance;

    public static DataSourceManager getInstance() {
        if (instance != null) {
            return instance;
        }

        synchronized (DataSourceConfigManager.class) {
            if (instance != null) {
                return instance;
            }

            DataSourceManager newInstance = new DataSourceManager();
            instance = newInstance;
        }
        return instance;
    }

    private DataSourceManager() {
        super();
    }

    @Override
    public String getConfigContent() {
        Resource resource = resourceLoader.getResource(DATASOURCE_CONF_KEY);
        String content = null;
        try (InputStream is = resource.getInputStream()) {
            content = IOUtils.toString(is);
        } catch (IOException e) {
            this.logger.error("ops!", e);
            throw new RuntimeException("error happened when ", e);
        }
        return content;
    }

    public static void main(String[] args) {

        System.setProperty(
                "datasourceConfigFile",
                "file:/Users/chouah/Documents/ws_game/pandora-game-server/src/main/resources/config/config.d/tencent_s2/config/nodes.conf");
        System.out.println(DataSourceManager.getInstance().getConfigContent());;

    }
}
