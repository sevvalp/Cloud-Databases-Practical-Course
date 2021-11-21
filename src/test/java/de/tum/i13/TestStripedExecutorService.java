package de.tum.i13;

import de.tum.i13.server.stripe.StripedExecutorService;
import de.tum.i13.server.stripe.StripedCallable;
import de.tum.i13.server.stripe.StripedRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Heinz Kabutz
 */
public class TestStripedExecutorService {
    @BeforeEach
    public void initialize() {
        TestRunnable.outOfSequence =
                TestUnstripedRunnable.outOfSequence =
                        TestFastRunnable.outOfSequence = false;
    }

    @Test
    public void testSingleStripeRunnable() throws InterruptedException {
        ExecutorService pool = new StripedExecutorService();
        Object stripe = new Object();
        AtomicInteger actual = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
            pool.submit(new TestRunnable(stripe, actual, i));
        }
        assertFalse(pool.isTerminated());
        assertFalse(pool.isShutdown());
        pool.shutdown();
        assertTrue(pool.awaitTermination(1, TimeUnit.HOURS));
        assertFalse(TestRunnable.outOfSequence, "Expected no out-of-sequence runnables to execute");
        assertTrue(pool.isTerminated());
    }

    @Test
    public void testShutdown() throws InterruptedException {
        ThreadGroup group = new ThreadGroup("stripetestgroup");
        Thread starter = new Thread(group, "starter") {
            public void run() {
                ExecutorService pool = new StripedExecutorService();
                Object stripe = new Object();
                AtomicInteger actual = new AtomicInteger(0);
                for (int i = 0; i < 100; i++) {
                    pool.submit(new TestRunnable(stripe, actual, i));
                }
                pool.shutdown();
            }
        };
        starter.start();
        starter.join();

        for (int i = 0; i < 100; i++) {
            if (group.activeCount() == 0) {
                return;
            }
            Thread.sleep(100);
        }

        assertEquals(0, group.activeCount());
    }

    @Test
    public void testShutdownNow() throws InterruptedException {
        ExecutorService pool = new StripedExecutorService();
        Object stripe = new Object();
        AtomicInteger actual = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
            pool.submit(new TestRunnable(stripe, actual, i));
        }
        Thread.sleep(500);
        assertFalse(pool.isTerminated());
        Collection<Runnable> unfinishedJobs = pool.shutdownNow();

        assertTrue(pool.isShutdown());
        assertTrue(pool.awaitTermination(1, TimeUnit.MINUTES));
        assertTrue(pool.isTerminated());

        assertTrue(unfinishedJobs.size() > 0);

        assertEquals(100, unfinishedJobs.size() + actual.intValue());
    }

    @Test
    public void testSingleStripeCallableWithCompletionService() throws InterruptedException, ExecutionException {
        ExecutorService pool = new StripedExecutorService();
        final CompletionService<Integer> cs = new ExecutorCompletionService<>(
                pool
        );

        Thread testSubmitter = new Thread("TestSubmitter") {
            public void run() {
                Object stripe = new Object();
                for (int i = 0; i < 50; i++) {
                    cs.submit(new TestCallable(stripe, i));
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    interrupt();
                }
                for (int i = 50; i < 100; i++) {
                    cs.submit(new TestCallable(stripe, i));
                }
            }
        };
        testSubmitter.start();

        for (int i = 0; i < 100; i++) {
            int actual = cs.take().get().intValue();
            //System.out.println("Retrieved " + actual);
            assertEquals(i, actual);
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(1, TimeUnit.HOURS));
        testSubmitter.join();
    }

    @Test
    public void testUnstripedRunnable() throws InterruptedException {
        ExecutorService pool = new StripedExecutorService();
        AtomicInteger actual = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
            pool.submit(new TestUnstripedRunnable(actual, i));
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(1, TimeUnit.HOURS));

        assertTrue(TestUnstripedRunnable.outOfSequence,
                "Expected at least some out-of-sequence runnables to execute");
    }

    @Test
    public void testMultipleStripes() throws InterruptedException {
        final ExecutorService pool = new StripedExecutorService();
        ExecutorService producerPool = Executors.newCachedThreadPool();
        for (int i = 0; i < 20; i++) {
            producerPool.submit(new Runnable() {
                public void run() {
                    Object stripe = new Object();
                    AtomicInteger actual = new AtomicInteger(0);
                    for (int i = 0; i < 100; i++) {
                        pool.submit(new TestRunnable(stripe, actual, i));
                    }
                }
            });
        }
        producerPool.shutdown();

        while (!producerPool.awaitTermination(1, TimeUnit.MINUTES)) {
            System.out.print("."); // checkstyle
        }

        pool.shutdown();
        assertTrue(pool.awaitTermination(1, TimeUnit.DAYS));
        assertFalse(TestRunnable.outOfSequence,
                "Expected no out-of-sequence runnables to execute");
    }


    @Test
    public void testMultipleFastStripes() throws InterruptedException {
        final ExecutorService pool = new StripedExecutorService();
        ExecutorService producerPool = Executors.newCachedThreadPool();
        for (int i = 0; i < 20; i++) {
            producerPool.submit(new Runnable() {
                public void run() {
                    Object stripe = new Object();
                    AtomicInteger actual = new AtomicInteger(0);
                    for (int i = 0; i < 100; i++) {
                        pool.submit(new TestFastRunnable(stripe, actual, i));
                    }
                }
            });
        }
        producerPool.shutdown();

        while (!producerPool.awaitTermination(1, TimeUnit.MINUTES)) {
            System.out.print("."); // checkstyle
        }

        pool.shutdown();
        assertTrue(pool.awaitTermination(1, TimeUnit.DAYS));
        assertFalse(TestFastRunnable.outOfSequence,
                "Expected no out-of-sequence runnables to execute");
    }


    public static class TestRunnable implements StripedRunnable {
        private final Object stripe;
        private final AtomicInteger stripeSequence;
        private final int expected;
        private static volatile boolean outOfSequence; // = false;

        public TestRunnable(Object stripe, AtomicInteger stripeSequence, int expected) {
            this.stripe = stripe;
            this.stripeSequence = stripeSequence;
            this.expected = expected;
        }

        public Object getStripe() {
            return stripe;
        }

        public void run() {
            try {
                ThreadLocalRandom rand = ThreadLocalRandom.current();
                Thread.sleep(rand.nextInt(10) + 10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int actual = stripeSequence.getAndIncrement();
            if (actual != expected) {
                outOfSequence = true;
            }
            //System.out.printf("Execute strip %h %d %d%n", stripe, actual, expected);
            assertEquals(actual, expected);
        }
    }

    public static class TestFastRunnable implements StripedRunnable {
        private final Object stripe;
        private final AtomicInteger stripeSequence;
        private final int expected;
        private static volatile boolean outOfSequence; // = false;

        public TestFastRunnable(Object stripe, AtomicInteger stripeSequence, int expected) {
            this.stripe = stripe;
            this.stripeSequence = stripeSequence;
            this.expected = expected;
        }

        public Object getStripe() {
            return stripe;
        }

        public void run() {
            int actual = stripeSequence.getAndIncrement();
            if (actual != expected) {
                outOfSequence = true;
            }
            //System.out.printf("Execute strip %h %d %d%n", stripe, actual, expected);
            assertEquals(actual, expected);
        }
    }

    public static class TestCallable implements StripedCallable<Integer> {
        private final Object stripe;
        private final int expected;

        public TestCallable(Object stripe, int expected) {
            this.stripe = stripe;
            this.expected = expected;
        }

        public Object getStripe() {
            return stripe;
        }

        public Integer call() throws Exception {
            try {
                ThreadLocalRandom rand = ThreadLocalRandom.current();
                Thread.sleep(rand.nextInt(10) + 10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return expected;
        }
    }

    public static class TestUnstripedRunnable implements Runnable {
        private final AtomicInteger stripeSequence;
        private final int expected;
        private static volatile boolean outOfSequence; // = false;

        public TestUnstripedRunnable(AtomicInteger stripeSequence, int expected) {
            this.stripeSequence = stripeSequence;
            this.expected = expected;
        }

        public void run() {
            try {
                ThreadLocalRandom rand = ThreadLocalRandom.current();
                Thread.sleep(rand.nextInt(10) + 10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int actual = stripeSequence.getAndIncrement();
            if (actual != expected) {
                outOfSequence = true;
            }
            //System.out.println("Execute unstriped " + actual + ", " + expected);
        }
    }


}
