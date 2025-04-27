package com.task.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class TaskExecutorServiceImpl implements TaskExecutor {
    BlockingQueue<TaskWrapper<?>> taskList;
    int maxPoolSize;
    ExecutorService executorService;
    AtomicBoolean shouldKeepRunning;
    Map<UUID, ReentrantLock> groupLocks;
    Map<UUID, AtomicBoolean> groupStatus;
    Thread consumeTasks;

    public TaskExecutorServiceImpl(int maxPoolSize){
        taskList = new LinkedBlockingQueue<>();
        this.maxPoolSize = maxPoolSize;
        executorService = Executors.newFixedThreadPool(maxPoolSize);
        shouldKeepRunning = new AtomicBoolean(true);
        groupLocks = new ConcurrentHashMap<>();
        groupStatus = new ConcurrentHashMap<>();
        consumeTasks = new Thread(this::taskConsumer);
        consumeTasks.start();
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        System.out.printf("Submitting Task group id: %s, Task id: %s\n",
                task.taskGroup().groupUUID(), task.taskUUID());
        taskList.offer(new TaskWrapper<>(task,future));
        return future;
    }
    @Override
    public void shutdown(){
        shouldKeepRunning.set(false);
        consumeTasks.interrupt();
        executeRemainingTasks();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void executeRemainingTasks() {
        List<TaskWrapper<?>> remainingTasks = new ArrayList<>();
        taskList.drainTo(remainingTasks);
        for (TaskWrapper<?> taskWrapper : remainingTasks) {
           executeTask(taskWrapper);
        }
    }

    public void taskConsumer(){
        while(shouldKeepRunning.get()){
            try{
                TaskWrapper<?> taskWrapper = taskList.take();
                System.out.printf("Consumed Task group id: %s, Task id: %s\n",
                        taskWrapper.task().taskGroup().groupUUID(), taskWrapper.task().taskUUID());

                executeTask(taskWrapper);
            }catch (InterruptedException e){
                System.out.println(e.getMessage());
            }
        }
    }

    public <T> void executeTask(TaskWrapper<T> taskWrapper){
        ReentrantLock lock = groupLocks.computeIfAbsent(taskWrapper.task().taskGroup().groupUUID()
                , id -> new ReentrantLock());
        var status = groupStatus.computeIfAbsent(taskWrapper.task().taskGroup().groupUUID()
                , id -> new AtomicBoolean(false));
        while (status.get()) {
            System.out.println("Lock for group " + taskWrapper.task().taskGroup() + " is currently held, stopping polling...");
            // Wait for some time before trying again
            try {
                Thread.sleep(500); // Avoid busy-waiting, sleep briefly
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
            executorService.submit(() -> {
                try {
                    lock.lock();
                   status.set(true);
                    System.out.printf("Acquired lock for task group id: %s and task id: %s\n",
                            taskWrapper.task().taskGroup(), taskWrapper.task().taskUUID());
                    Thread.sleep(5000);
                    T result = taskWrapper.task().taskAction().call();
                    taskWrapper.future().complete(result);
                } catch (Exception e) {
                    taskWrapper.future().completeExceptionally(e);
                    System.out.println("Task execution failed for group " + taskWrapper.task().taskGroup() + ": " + e.getMessage());
                } finally {
                    System.out.println("Release lock for group id:" + taskWrapper.task().taskGroup()+" task id "+ taskWrapper.task().taskUUID());
                    lock.unlock();
                    status.set(false);
                }
            });
    }
}
