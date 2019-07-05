/*
 *
 *   Copyright 2019 Weibo, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.weibo.breeze.autoscan;

import com.weibo.breeze.Breeze;
import com.weibo.breeze.annotation.BreezeSchema;
import com.weibo.breeze.annotation.BreezeSerializer;
import com.weibo.breeze.message.SchemaDesc;
import com.weibo.breeze.serializer.Serializer;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @author zhanglei28
 * @date 2019/4/1.
 */
public class BreezeScan {
    private static final Logger logger = LoggerFactory.getLogger(BreezeScan.class);
    private String[] packageNames;

    public BreezeScan(String packageName) {
        this.packageNames = new String[]{packageName};
    }

    public BreezeScan(String[] packageNames) {
        this.packageNames = packageNames;
    }

    public void scan() {
        for (String packageName : packageNames) {
            Reflections reflections = new Reflections(packageName);
            // process serializer
            try {
                Set<Class<?>> set = reflections.getTypesAnnotatedWith(BreezeSerializer.class);
                for (Class clz : set) {
                    if (Serializer.class.isAssignableFrom(clz)) {
                        try {
                            Breeze.registerSerializer((Serializer) clz.newInstance());
                        } catch (Exception e) {
                            logger.warn("register breeze serializer fail. class:{}, e:{}", clz.getName(), e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("scan breeze serializer fail. e:" + e.getMessage());
            }

            // process BreezeSchema
            try {
                Set<Class<?>> set = reflections.getTypesAnnotatedWith(BreezeSchema.class);
                for (Class clz : set) {
                    if (SchemaDesc.class.isAssignableFrom(clz)) {
                        try {
                            Breeze.registerSchema((SchemaDesc) clz.newInstance());
                        } catch (Exception e) {
                            logger.warn("register breeze serializer fail. class:{}, e:{}", clz.getName(), e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("scan breeze schema fail. e:" + e.getMessage());
            }
        }
    }
}
