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

package com.weibo.breeze.spring;

import com.weibo.breeze.Breeze;
import com.weibo.breeze.message.SchemaDesc;
import com.weibo.breeze.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * @author zhanglei28
 * @date 2019/4/1.
 */
public class BreezeAutoRegisterProcessor implements BeanPostProcessor {
    private static final Logger logger = LoggerFactory.getLogger(BreezeAutoRegisterProcessor.class);

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> clz = bean.getClass();
        if (Serializer.class.isAssignableFrom(clz)) {
            try {
                Breeze.registerSerializer((Serializer) bean);
            } catch (Exception e) {
                logger.warn("BreezeAutoRegisterProcessor register breeze serializer fail. class:{}, e:{}", clz.getName(), e.getMessage());
            }
        } else if (SchemaDesc.class.isAssignableFrom(clz)) {
            try {
                Breeze.registerSchema((SchemaDesc) bean);
            } catch (Exception e) {
                logger.warn("BreezeAutoRegisterProcessor register breeze serializer fail. class:{}, e:{}", clz.getName(), e.getMessage());
            }
        }
        return bean;
    }
}
