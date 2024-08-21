package edu.cmu.pdl.macaronsimulator.common;

import java.util.function.BiConsumer;

public class TaskParallelRunner {
    public static void run(BiConsumer<Integer, Integer> task) {
        Thread threads[] = new Thread[Runtime.getRuntime().availableProcessors()];
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                task.accept(threadId, threads.length);
            });
            threads[i].start();
        }

        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
