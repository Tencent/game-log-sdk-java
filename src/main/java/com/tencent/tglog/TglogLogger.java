package com.tencent.tglog;

/**
 * Logger interface for TGLog debug logging.
 */
public interface TglogLogger {

    enum Level {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    /**
     * Log a message at the specified level.
     *
     * @param level Log level
     * @param message Log message
     */
    void log(Level level, String message);

    /**
     * Log a message with throwable.
     *
     * @param level Log level
     * @param message Log message
     * @param throwable Exception
     */
    default void log(Level level, String message, Throwable throwable) {
        log(level, message + ": " + throwable.getMessage());
    }

    /**
     * Default logger that prints to System.out/err.
     */
    class DefaultLogger implements TglogLogger {
        @Override
        public void log(Level level, String message) {
            String prefix = String.format("[TGLog-%s] ", level);
            if (level == Level.ERROR) {
                System.err.println(prefix + message);
            } else {
                System.out.println(prefix + message);
            }
        }
    }

    /**
     * No-op logger.
     */
    class NoOpLogger implements TglogLogger {
        @Override
        public void log(Level level, String message) {
            // Do nothing
        }
    }
}
