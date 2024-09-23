package com.github.zzf.micrometer.config.aop;

import io.micrometer.core.aop.TimedAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * "@Timed" aop 切面
 *
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/9/27
 */
@Configuration
@EnableAspectJAutoProxy
public class TimedAopConfiguration {

    @Bean
    public TimedAspect timedAspect() {
        return new TimedAspect();
    }

}
