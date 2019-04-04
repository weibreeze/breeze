package com.weibo.breeze.spring;

import com.weibo.breeze.Breeze;
import com.weibo.breeze.message.SchemaDesc;
import com.weibo.breeze.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * Created by zhanglei28 on 2019/4/1.
 */
public class BreezeAutoRegisterProcessor implements BeanPostProcessor{
    private static final Logger logger = LoggerFactory.getLogger(BreezeAutoRegisterProcessor.class);

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> clz = bean.getClass();
        if (Serializer.class.isAssignableFrom(clz)) {
            try {
                Breeze.registerSerializer((Serializer)bean);
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
