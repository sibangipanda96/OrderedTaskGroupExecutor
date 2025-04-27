package com.task.executor;

import java.util.concurrent.CompletableFuture;

public record TaskWrapper<T>(Task<T> task, CompletableFuture<T> future) {
}
