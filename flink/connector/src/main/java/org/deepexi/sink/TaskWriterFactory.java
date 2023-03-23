package org.deepexi.sink;

import org.apache.iceberg.io.TaskWriter;

import java.io.Serializable;

public interface TaskWriterFactory<T> extends Serializable {
    void initialize(int taskId, int attemptId);
    TarimTaskWriter<T> create();
}
