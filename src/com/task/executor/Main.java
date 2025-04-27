package com.task.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

public class Main {
    public static void main(String[] args) {
        int maxConcurrency = 3;
        TaskExecutor taskExecutorService = new TaskExecutorServiceImpl(maxConcurrency);
        try {
            List<TaskGroup> taskGroupList = new ArrayList<>();
            List<Future> outcome = new ArrayList<>();
            for (int i = 1; i <= 4; i++) {
                taskGroupList.add(new TaskGroup(UUID.randomUUID()));
            }
            for (int i = 1; i <= 20; i++) {
                TaskGroup group = taskGroupList.get((i-1) % 4);
                TaskType taskType = (i % 3 == 0) ? TaskType.READ : TaskType.WRITE;
                UUID taskId = UUID.randomUUID();
                int finalI = i;
                outcome.add(
                        taskExecutorService.submitTask(new Task<>(taskId, group, taskType,
                                () -> {
                                    Thread.sleep(1000*(finalI%4));
                                    return String.format("Executing Task %s of type %s from group %s"
                                            , taskId, taskType, group);
                                }))
                );
            }
            for (Future future : outcome) {
                try {
                    System.out.println(future.get());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            taskExecutorService.shutdown();
        }

    }
}
