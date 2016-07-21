package com.ft.message.consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;


public class LoggingTestHelper {
    private static final String APPENDER = "MockAppender";
    
    private static final Map<Class<?>,Level> ORIGINAL_LEVELS = new HashMap<>();
    
    public static Logger configureMockAppenderFor(Class<?> clazz) {
        Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(clazz);

        Appender<ILoggingEvent> appender = mock(Appender.class);
        when(appender.getName()).thenReturn(APPENDER);
        logger.addAppender(appender);
        
        ORIGINAL_LEVELS.put(clazz, logger.getLevel());
        logger.setLevel(Level.DEBUG);
        
        return logger;
    }

    public static void resetLoggingFor(Class<?> clazz) {
        Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(clazz);
        Appender<ILoggingEvent> appender = logger.getAppender(APPENDER);
        logger.detachAppender(appender);
        
        Level logLevel = ORIGINAL_LEVELS.remove(clazz);
        if (logLevel != null) {
            logger.setLevel(logLevel);
        }
    }
    
    public static void assertLogEvent(Logger logger, String regex, Level... level) {
      assertLogEvent(logger, regex, true, level);
    }
    
    public static void assertNoLogEvent(Logger logger, String regex, Level... level) {
      assertLogEvent(logger, regex, false, level);
    }
    
    public static void assertLogEvent(Logger logger, String regex, boolean matching, Level... level) {
        Appender<ILoggingEvent> appender = logger.getAppender(APPENDER);
        ArgumentCaptor<LoggingEvent> argument = ArgumentCaptor.forClass(LoggingEvent.class);
        verify(appender, atLeastOnce()).doAppend(argument.capture());
        
        StringBuilder sb = new StringBuilder(regex);
        if (!regex.startsWith("^")) {
            sb.insert(0, ".*");
        }
        if (!regex.endsWith("$")) {
            sb.append(".*");
        }
        final String pattern = sb.toString();
        
        LoggingEvent actual = null;
        Stream<LoggingEvent> stream = argument.getAllValues().stream()
                .filter(event -> event.getFormattedMessage().matches(pattern)
                                  || (event.getThrowableProxy() != null && event.getThrowableProxy().getClassName().matches(pattern)));
        
        if (matching) {
          actual = stream.findFirst().get();
          
          if ((level != null) && (level.length > 0)) {
            assertThat(actual.getLevel(), equalTo(level[0]));
          }
        }
        else {
          assertThat(stream.findFirst().isPresent(), equalTo(false));
        }
    }
}
