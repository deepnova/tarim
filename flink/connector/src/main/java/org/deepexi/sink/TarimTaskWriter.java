package org.deepexi.sink;

import org.apache.iceberg.io.WriteResult;
import org.deepexi.WResult;

import java.io.Closeable;
import java.io.IOException;

public interface TarimTaskWriter<T> extends Closeable {
    void write(T row) throws IOException;

    WResult complete() throws IOException;

}
