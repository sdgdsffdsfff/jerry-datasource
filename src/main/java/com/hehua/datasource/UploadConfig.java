/**
 * 
 */
package com.hehua.datasource;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.hehua.framework.config.ZookeeperConfigManager;

/**
 * @author zhihua
 *
 */
public class UploadConfig {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        String value = FileUtils
                .readFileToString(new File(
                        "/Users/zhihua/Documents/workspace/hehua-datasource/src/main/resources/config/nodes.conf"));
        ZookeeperConfigManager.getInstance().setString("datasource", value);
    }

}
