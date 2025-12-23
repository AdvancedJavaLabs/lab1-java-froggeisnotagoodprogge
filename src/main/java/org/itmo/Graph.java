package org.itmo;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;

class Graph {
    private final int V;
    private final ArrayList<Integer>[] adjList;

    Graph(int vertices) {
        this.V = vertices;
        adjList = new ArrayList[vertices];
        for (int i = 0; i < vertices; ++i) {
            adjList[i] = new ArrayList<>();
        }
    }

    void addEdge(int src, int dest) {
        if (!adjList[src].contains(dest)) {
            adjList[src].add(dest);
        }
    }

    void parallelBFS(int startVertex, Integer threadsCount) {
        int numThreads = (threadsCount == null) ? Runtime.getRuntime().availableProcessors() : threadsCount;
        AtomicIntegerArray visited = new AtomicIntegerArray(V);
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        ConcurrentLinkedQueue<Integer> frontier = new ConcurrentLinkedQueue<>();

        for (int v = 0; v < V; v++) {
            visited.set(v, 0);
        }

        frontier.add(startVertex);
        visited.set(startVertex, 1);

        try {
            while (!frontier.isEmpty()) {
                int tasks = Math.min(numThreads, frontier.size());
                CountDownLatch latch = new CountDownLatch(tasks);
                List<Integer>[] nextFrontiers = new ArrayList[tasks];

                for (int t = 0; t < tasks; t++) {
                    final int taskId = t;
                    pool.submit(() -> {
                        List<Integer> nextLevel = new ArrayList<>();
                        try {
                            Integer node;
                            while ((node = frontier.poll()) != null) {
                                for (int neighbor : adjList[node]) {
                                    if (visited.get(neighbor) == 0 && visited.compareAndSet(neighbor, 0, 1)) {
                                        nextLevel.add(neighbor);
                                    }
                                }
                            }
                        } finally {
                            nextFrontiers[taskId] = nextLevel;
                            latch.countDown();
                        }
                    });
                }

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                for (List<Integer> level : nextFrontiers) {
                    frontier.addAll(level);
                }
            }
        } finally {
            pool.shutdown();
        }
    }

    void parallelBFSUnsafe(int startVertex, Integer threadsCount) {
        int numThreads = (threadsCount == null) ? Runtime.getRuntime().availableProcessors() : threadsCount;
        AtomicIntegerArray visited = new AtomicIntegerArray(V);
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        ConcurrentLinkedQueue<Integer> frontier = new ConcurrentLinkedQueue<>();

        for (int v = 0; v < V; v++) visited.set(v, 0);

        frontier.add(startVertex);
        visited.set(startVertex, 1);

        try {
            while (!frontier.isEmpty()) {
                int tasks = Math.min(numThreads, frontier.size());
                CountDownLatch latch = new CountDownLatch(tasks);
                List<Integer>[] nextFrontiers = new ArrayList[tasks];

                for (int t = 0; t < tasks; t++) {
                    final int taskId = t;
                    pool.submit(() -> {
                        List<Integer> nextLevel = new ArrayList<>();
                        try {
                            Integer node;
                            while ((node = frontier.poll()) != null) {
                                for (int neighbor : adjList[node]) {
                                    if (visited.get(neighbor) == 0 && visited.compareAndSet(neighbor, 0, 1)) {
                                        nextLevel.add(neighbor);
                                    }
                                }
                            }
                        } finally {
                            nextFrontiers[taskId] = nextLevel;
                            latch.countDown();
                        }
                    });
                }

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                for (List<Integer> level : nextFrontiers) frontier.addAll(level);
            }
        } finally {
            pool.shutdown();
        }
    }

//    void parallelBFS(int startVertex) {
//        int numThreads = Runtime.getRuntime().availableProcessors();
//        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
//
//        AtomicBoolean[] visited = new AtomicBoolean[V];
//        for (int i = 0; i < V; i++)
//            visited[i] = new AtomicBoolean(false);
//
//        LevelQueues levelQueues = new LevelQueues();
//
//        visited[startVertex].set(true);
//        levelQueues.current.add(startVertex);
//
//        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1); // +1 — главный поток
//        AtomicInteger remaining = new AtomicInteger();
//
//        for (int t = 0; t < numThreads; t++) {
//            executor.submit(() -> {
//                while (true) {
//                    try {
//                        barrier.await();
//                    } catch (Exception e) {
//                        System.out.println("barrier.await broken");
//                    }
//
//                    if (levelQueues.current.isEmpty())
//                        barrier.await();
//
//                    Integer v;
//                    while ((v = levelQueues.current.poll()) != null) {
//                        for (int n : adjList[v]) {
//                            if (visited[n].compareAndSet(false, true)) {
//                                levelQueues.next.add(n);
//                            }
//                        }
//                    }
//
//                    if (remaining.decrementAndGet() == 0) {
//                        System.out.println("Level process ended");
//                    }
//
//                    try {
//                        barrier.await();
//                    } catch (Exception ignored) {
//                    }
//                }
//            });
//        }
//
//        try {
//            while (!levelQueues.current.isEmpty()) {
//                remaining.set(numThreads);
//                barrier.await();
//                barrier.await();
//
//                levelQueues.current = levelQueues.next;
//                levelQueues.next = new ConcurrentLinkedQueue<>();
//            }
//        } catch (Exception e) {
//            System.out.println("main process barrier.await broken");
//        }
//
//        executor.shutdownNow();
//    }
//
//    void parallelBFS_2(int startVertex) {
//        ExecutorService pool = Executors.newFixedThreadPool(
//                Runtime.getRuntime().availableProcessors());
//
//        AtomicBoolean[] visited = new AtomicBoolean[V];
//        for (int i = 0; i < V; i++)
//            visited[i] = new AtomicBoolean(false);
//
//        ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();
//        queue.add(startVertex);
//        visited[startVertex].set(true);
//
//        while (!queue.isEmpty()) {
//            Integer v = queue.poll();
//            if (v == null) continue;
//
//            CompletableFuture.runAsync(() -> {
//                for (int n : adjList[v]) {
//                    if (visited[n].compareAndSet(false, true)) {
//                        queue.add(n);
//                    }
//                }
//            }, pool);
//        }
//
//        pool.shutdown();
//    }
//
//    private class LevelQueues {
//        public ConcurrentLinkedQueue<Integer> current = new ConcurrentLinkedQueue<>();
//        public ConcurrentLinkedQueue<Integer> next = new ConcurrentLinkedQueue<>();
//    }

    //Generated by ChatGPT
    void bfs(int startVertex) {
        boolean[] visited = new boolean[V];

        LinkedList<Integer> queue = new LinkedList<>();

        visited[startVertex] = true;
        queue.add(startVertex);

        while (!queue.isEmpty()) {
            startVertex = queue.poll();     // конкуренция за queue

            for (int n : adjList[startVertex]) {
                if (!visited[n]) {      // здесь гонка
                    visited[n] = true;
                    queue.add(n);       // риск многократного добавления n
                }
            }
        }
    }

}
