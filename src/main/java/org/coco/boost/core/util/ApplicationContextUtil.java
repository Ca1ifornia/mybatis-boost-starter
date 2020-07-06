package org.coco.boost.core.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 *
 * @author coco
 */
@Component("applicationContextUtil")
public class ApplicationContextUtil implements ApplicationContextAware {

  private static ApplicationContext applicationContext;

  public static ApplicationContext getApplicationContext() {
    return applicationContext;
  }

  public static <T> T getBean(Class<T> cls) {
    return applicationContext.getBean(cls);
  }

  public static <T> T getBeanByName(String name,Class<T> cls) {
    return applicationContext.getBean(name,cls);
  }

  public boolean containsBean(String name) {
    return applicationContext.containsBean(name);
  }

  public static Environment getEnvironment() {
    return applicationContext.getEnvironment();
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    applicationContext = applicationContext;
  }
}
