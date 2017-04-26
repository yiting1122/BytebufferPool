package com.yiting.pool;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hzyiting on 2017/4/25.
 */
public interface BufferPool {
	ByteBuffer allocate(int size);
	void recycle(ByteBuffer buffer);
	long capacity();
	long size();
	int getConReadBufferChunk();
	int getSharedOptsCount();
	int getChunkSize();
	ConcurrentHashMap<Long,Long> getNetDirectMemoryUsage();
}
