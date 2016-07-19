package com.ft.message.consumer.health;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.ft.message.consumer.config.HealthcheckConfiguration;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.platform.dropwizard.AdvancedResult;
import com.ft.platform.dropwizard.AdvancedResult.Status;

@RunWith(MockitoJUnitRunner.class)
public class PassiveMessageQueueProxyConsumerHealthcheckTest {
  private static final String NAME = "Test Healthcheck";
  private static final int SEVERITY = 2;
  private static final String BUSINESS_IMPACT = "Test Business Impact";
  private static final String TECH_SUMMARY = "Test Tech Summary";
  private static final String PANIC_GUIDE = "http://panic-guide.example.org/";
  
  private PassiveMessageQueueProxyConsumerHealthcheck healthcheck;
  private HealthcheckConfiguration config = new HealthcheckConfiguration(
      NAME, SEVERITY, BUSINESS_IMPACT, TECH_SUMMARY, PANIC_GUIDE);
  
  @Mock
  private MessageQueueProxyService service;
  
  @Before
  public void setUp() {
    healthcheck = new PassiveMessageQueueProxyConsumerHealthcheck(config, service);
  }
  
  @Test
  public void thatHealthcheckConfigurationIsUsed() {
    assertThat(healthcheck.severity(), equalTo(SEVERITY));
    assertThat(healthcheck.businessImpact(), equalTo(BUSINESS_IMPACT));
    assertThat(healthcheck.technicalSummary(), equalTo(TECH_SUMMARY));
    assertThat(healthcheck.panicGuideUrl(), equalTo(PANIC_GUIDE));
  }
  
  @Test
  public void thatHealthcheckIsHealthyWhenMessagesHaveBeenRead() {
    String message = String.format(MessageQueueProxyService.MESSAGES_CONSUMED, 42);
    when(service.getStatus()).thenReturn(message);
    
    AdvancedResult actual = healthcheck.executeAdvanced();
    assertThat(actual.status(), equalTo(Status.OK));
    assertThat(actual.checkOutput(), equalTo(message));
  }
  
  @Test
  public void thatHealthcheckIsHealthyWhenNoMessagesHaveBeenRead() {
    String message = String.format(MessageQueueProxyService.MESSAGES_CONSUMED, 0);
    when(service.getStatus()).thenReturn(message);
    
    AdvancedResult actual = healthcheck.executeAdvanced();
    assertThat(actual.status(), equalTo(Status.OK));
    assertThat(actual.checkOutput(), equalTo(message));
  }
  
  @Test
  public void thatHealthcheckIsUnhealthyWhenServiceHasAnErrorCondition() {
    String message = "Something is wrong";
    when(service.getStatus()).thenReturn(message);
    
    AdvancedResult actual = healthcheck.executeAdvanced();
    assertThat(actual.status(), equalTo(Status.ERROR));
    assertThat(actual.checkOutput(), equalTo(message));
  }
}
