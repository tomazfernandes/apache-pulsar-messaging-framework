package com.github.tomazfernandes;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public enum FailureHandlingMode {

    NACK,

    SEEK_LAST_ACKNOWLEDGED,

    SEEK_LAST_ACKNOWLEDGED_BY_KEY,

    SEEK_CURRENT,

    IGNORE

}
