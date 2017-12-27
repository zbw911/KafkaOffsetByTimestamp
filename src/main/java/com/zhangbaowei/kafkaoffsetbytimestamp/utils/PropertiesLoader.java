//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.zhangbaowei.kafkaoffsetbytimestamp.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Properties;

public class PropertiesLoader {
    private static Logger logger = LoggerFactory.getLogger(PropertiesLoader.class);
    private static ResourceLoader resourceLoader = new DefaultResourceLoader();
    private final Properties properties;

    public PropertiesLoader(String... resourcesPaths) {
        this.properties = this.loadProperties(resourcesPaths);
    }

    public Properties getProperties() {
        return this.properties;
    }

    private String getValue(String key) {
        String systemProperty = System.getProperty(key);
        if (systemProperty != null) {
            return systemProperty;
        } else {
            return this.properties.containsKey(key) ? this.properties.getProperty(key) : "";
        }
    }

    public String getProperty(String key) {
        String value = this.getValue(key);
        if (value == null) {
            throw new NoSuchElementException();
        } else {
            return value;
        }
    }

    public String getProperty(String key, String defaultValue) {
        String value = this.getValue(key);
        return value != null ? value : defaultValue;
    }

    public Integer getInteger(String key) {
        String value = this.getValue(key);
        if (value == null) {
            throw new NoSuchElementException();
        } else {
            return Integer.valueOf(value);
        }
    }

    public Integer getInteger(String key, Integer defaultValue) {
        String value = this.getValue(key);
        return value != null ? Integer.valueOf(value) : defaultValue;
    }

    public Double getDouble(String key) {
        String value = this.getValue(key);
        if (value == null) {
            throw new NoSuchElementException();
        } else {
            return Double.valueOf(value);
        }
    }

    public Double getDouble(String key, Integer defaultValue) {
        String value = this.getValue(key);
        return value != null ? Double.valueOf(value) : (double) defaultValue;
    }

    public Boolean getBoolean(String key) {
        String value = this.getValue(key);
        if (value == null) {
            throw new NoSuchElementException();
        } else {
            return Boolean.valueOf(value);
        }
    }

    public Boolean getBoolean(String key, boolean defaultValue) {
        String value = this.getValue(key);
        return value != null ? Boolean.valueOf(value) : defaultValue;
    }

    private Properties loadProperties(String... resourcesPaths) {
        Properties props = new Properties();
        String[] var3 = resourcesPaths;
        int var4 = resourcesPaths.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            String location = var3[var5];
            InputStream is = null;

            try {
                Resource resource = resourceLoader.getResource(location);
                is = resource.getInputStream();
                props.load(is);
            } catch (IOException var12) {
                logger.info("Could not load properties from path:" + location + ", " + var12.getMessage());
            } finally {
//                IOUtils.closeQuietly(is);//
//// Source code recreated from a .class file by IntelliJ IDEA
//// (powered by Fernflower decompiler)
////
//
//package com.autonews.comm.utils;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.NoSuchElementException;
//import java.util.Properties;
//import org.apache.commons.io.IOUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.core.io.DefaultResourceLoader;
//import org.springframework.core.io.Resource;
//import org.springframework.core.io.ResourceLoader;
//
//public class PropertiesLoader {
//    private static Logger logger = LoggerFactory.getLogger(PropertiesLoader.class);
//    private static ResourceLoader resourceLoader = new DefaultResourceLoader();
//    private final Properties properties;
//
//    public PropertiesLoader(String... resourcesPaths) {
//        this.properties = this.loadProperties(resourcesPaths);
//    }
//
//    public Properties getProperties() {
//        return this.properties;
//    }
//
//    private String getValue(String key) {
//        String systemProperty = System.getProperty(key);
//        if (systemProperty != null) {
//            return systemProperty;
//        } else {
//            return this.properties.containsKey(key) ? this.properties.getProperty(key) : "";
//        }
//    }
//
//    public String getProperty(String key) {
//        String value = this.getValue(key);
//        if (value == null) {
//            throw new NoSuchElementException();
//        } else {
//            return value;
//        }
//    }
//
//    public String getProperty(String key, String defaultValue) {
//        String value = this.getValue(key);
//        return value != null ? value : defaultValue;
//    }
//
//    public Integer getInteger(String key) {
//        String value = this.getValue(key);
//        if (value == null) {
//            throw new NoSuchElementException();
//        } else {
//            return Integer.valueOf(value);
//        }
//    }
//
//    public Integer getInteger(String key, Integer defaultValue) {
//        String value = this.getValue(key);
//        return value != null ? Integer.valueOf(value) : defaultValue;
//    }
//
//    public Double getDouble(String key) {
//        String value = this.getValue(key);
//        if (value == null) {
//            throw new NoSuchElementException();
//        } else {
//            return Double.valueOf(value);
//        }
//    }
//
//    public Double getDouble(String key, Integer defaultValue) {
//        String value = this.getValue(key);
//        return value != null ? Double.valueOf(value) : (double)defaultValue;
//    }
//
//    public Boolean getBoolean(String key) {
//        String value = this.getValue(key);
//        if (value == null) {
//            throw new NoSuchElementException();
//        } else {
//            return Boolean.valueOf(value);
//        }
//    }
//
//    public Boolean getBoolean(String key, boolean defaultValue) {
//        String value = this.getValue(key);
//        return value != null ? Boolean.valueOf(value) : defaultValue;
//    }
//
//    private Properties loadProperties(String... resourcesPaths) {
//        Properties props = new Properties();
//        String[] var3 = resourcesPaths;
//        int var4 = resourcesPaths.length;
//
//        for(int var5 = 0; var5 < var4; ++var5) {
//            String location = var3[var5];
//            InputStream is = null;
//
//            try {
//                Resource resource = resourceLoader.getResource(location);
//                is = resource.getInputStream();
//                props.load(is);
//            } catch (IOException var12) {
//                logger.info("Could not load properties from path:" + location + ", " + var12.getMessage());
//            } finally {
//                IOUtils.closeQuietly(is);
//            }
//        }
//
//        return props;
//    }
//}

                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        ;
                    }
                }
            }
        }

        return props;
    }
}
