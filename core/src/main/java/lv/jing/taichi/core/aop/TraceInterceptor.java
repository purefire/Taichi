package lv.jing.taichi.core.aop;

import lv.jing.taichi.anno.Trace;
import java.lang.invoke.MethodHandles;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jing.lv
 */
public class TraceInterceptor implements MethodInterceptor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public Object invoke(MethodInvocation methodInvocation) throws Throwable {
    final Trace trace = methodInvocation.getMethod().getAnnotation(Trace.class);
    final long threshold = trace == null ? 0L : trace.threshold();
    final long begin = System.currentTimeMillis();
    final Object ret = methodInvocation.proceed();
    final long cost = System.currentTimeMillis() - begin;
    if (cost > threshold) {
      log.info(
        "***** TRACE: cost={}ms, threshold={}ms, method={}#{} *****",
        cost,
        threshold,
        methodInvocation.getMethod().getDeclaringClass().getName(),
        methodInvocation.getMethod().getName(),
        cost
      );
    }
    return ret;
  }

}
