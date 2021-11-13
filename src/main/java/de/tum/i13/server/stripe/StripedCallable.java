package de.tum.i13.server.stripe;

import java.util.concurrent.Callable;

/**
 * From https://www.javaspecialists.eu/archive/Issue206-Striped-Executor-Service.html
 *
 * Interface for a Callable to identify the stripe (=key) of an operation.
 * @version 0.1
 * @since   2021-11-13
 */
public interface StripedCallable<V> extends Callable<V>, StripedObject {
}
