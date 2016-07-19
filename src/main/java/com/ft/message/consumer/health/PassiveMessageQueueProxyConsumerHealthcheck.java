package com.ft.message.consumer.health;

import java.util.regex.Pattern;

import com.ft.message.consumer.config.HealthcheckConfiguration;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.platform.dropwizard.AdvancedHealthCheck;
import com.ft.platform.dropwizard.AdvancedResult;

public class PassiveMessageQueueProxyConsumerHealthcheck
    extends AdvancedHealthCheck {

  private static final Pattern OK_STATUS = Pattern.compile(
      MessageQueueProxyService.MESSAGES_CONSUMED.replace("(", "\\(")
                                                .replace(")", "\\)")
                                                .replace("%s", "\\d+")
                                                .replace(".", "\\."));
  
  private final HealthcheckConfiguration healthcheckConfiguration;
  private final MessageQueueProxyService proxyService;
  
  public PassiveMessageQueueProxyConsumerHealthcheck(
      final HealthcheckConfiguration healthcheckConfiguration,
      final MessageQueueProxyService proxyService) {
    
    super(healthcheckConfiguration.getName());
    this.healthcheckConfiguration = healthcheckConfiguration;
    this.proxyService = proxyService;
  }
  
  @Override
  protected AdvancedResult checkAdvanced() throws Exception {
    String status = proxyService.getStatus();
    
    if (OK_STATUS.matcher(status).matches()) {
      return AdvancedResult.healthy(status);
    }
    return AdvancedResult.error(this, status);
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
