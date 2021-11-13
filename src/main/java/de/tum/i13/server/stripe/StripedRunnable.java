package de.tum.i13.server.stripe;

/**
 * From https://www.javaspecialists.eu/archive/Issue206-Striped-Executor-Service.html
 *
 * Interface for Runnables, to identify the stripe (=key) of an operation.
 * @version 0.1
 * @since   2021-11-13
 */
public interface StripedRunnable extends Runnable, StripedObject {
}
