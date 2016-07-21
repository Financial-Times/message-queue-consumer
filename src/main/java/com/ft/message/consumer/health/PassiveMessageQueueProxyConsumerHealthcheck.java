package com.ft.message.consumer.health;

import java.util.regex.Pattern;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ft.message.consumer.config.HealthcheckConfiguration;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.platform.dropwizard.AdvancedHealthCheck;
import com.ft.platform.dropwizard.AdvancedResult;

/** When the application consumer is polling frequently or the topic may contain
 *  many/large messages, a passive health check that simply monitors the most recent
 *  read operation is less disruptive and does not reduce significantly the accuracy
 *  of the health check result.
 *  @author keith.hatton
 */
public class PassiveMessageQueueProxyConsumerHealthcheck
    extends AdvancedHealthCheck {

  private static final Pattern OK_STATUS = Pattern.compile(
      MessageQueueProxyService.MESSAGES_CONSUMED.replace("(", "\\(")
                                                .replace(")", "\\)")
                                                .replace("%s", "\\d+")
                                                .replace(".", "\\."));
  
  private final HealthcheckConfiguration healthcheckConfiguration;
  private final MessageQueueProxyService proxyService;
  private Timer timer;
  
  public PassiveMessageQueueProxyConsumerHealthcheck(
      final HealthcheckConfiguration healthcheckConfiguration,
      final MessageQueueProxyService proxyService,
      final MetricRegistry metrics) {
    
    super(healthcheckConfiguration.getName());
    this.healthcheckConfiguration = healthcheckConfiguration;
    this.proxyService = proxyService;
    
    if (metrics != null) {
      timer = metrics.timer(
          MetricRegistry.name(PassiveMessageQueueProxyConsumerHealthcheck.class, "checkAdvanced"));
    } else {
      timer = new Timer();
    }
  }
  
  @Override
  protected AdvancedResult checkAdvanced() throws Exception {
    try (Timer.Context ctx = timer.time()) {
      String status = proxyService.getStatus();
      
      if (OK_STATUS.matcher(status).matches()) {
        return AdvancedResult.healthy(status);
      }
      
      return AdvancedResult.error(this, status);
    }
  }

  @Override
  protected int severity() {
    return healthcheckConfiguration.getSeverity();
  }

  @Override
  protected String businessImpact() {
    return healthcheckConfiguration.getBusinessImpact();
  }

  @Override
  protected String technicalSummary() {
    return healthcheckConfiguration.getTechnicalSummary();
  }

  @Override
  protected String panicGuideUrl() {
    return healthcheckConfiguration.getPanicGuideUrl();
  }
}
