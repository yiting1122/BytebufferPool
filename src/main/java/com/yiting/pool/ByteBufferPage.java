package com.yiting.pool;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hzyiting on 2017/4/25.
 */
public class ByteBufferPage {
	private final ByteBuffer buf;
	private final int chunkSize;
	private final int chunkCount;
	private final BitSet chunkAllocateTrack;
	private final AtomicBoolean allocLockStatus = new AtomicBoolean(false);
	private final long startAddress;
	
	public ByteBufferPage(ByteBuffer buf, int chunkSize) {
		this.chunkSize = chunkSize;
		chunkCount = buf.capacity() / chunkSize;
		chunkAllocateTrack = new BitSet(chunkCount);
		this.buf = buf;
		startAddress = ((sun.nio.ch.DirectBuffer) buf).address();
	}
	
	public ByteBuffer allockChunk(int theChunkCount) {
		if (!allocLockStatus.compareAndSet(false, true)) {
			return null;
		}
		int startChunk = -1;
		int continueCount = 0;
		try {
			for (int i = 0; i < chunkCount; i++) {
				if (chunkAllocateTrack.get(i) == false) {
					if (startChunk == -1) {
						startChunk = i;
						continueCount = 1;
						if (theChunkCount == 1) {
							break;
						}
					} else {
						if (++continueCount == theChunkCount) {
							break;
						}
					}
				} else {
					startChunk = -1;
					continueCount = 0;
				}
			}
			
			if (continueCount == theChunkCount) {
				int offsetStart = startChunk * chunkSize;
				int offsetEnd = offsetStart + theChunkCount * chunkSize;
				buf.limit(offsetEnd);
				buf.position(offsetStart);
				ByteBuffer newBuf = buf.slice();
				makeChunksUsed(startChunk, theChunkCount);
				return newBuf;
			} else {
				return null;
			}
		} finally {
			allocLockStatus.set(false);
		}
	}
	
	public boolean recycleBuffer(ByteBuffer parent, int startChunk, int theChunkCount) {
		if (parent == this.buf) {
			while (!this.allocLockStatus.compareAndSet(false, true)) {
				Thread.yield();
			}
			try {
				markChunksUnused(startChunk, theChunkCount);
			} finally {
				this.allocLockStatus.set(false);
			}
			return true;
		}
		return false;
	}
	
	private void makeChunksUsed(int startChunk, int theChunkCount) {
		for (int i = 0; i < theChunkCount; i++) {
			chunkAllocateTrack.set(startChunk + i);
		}
	}
	
	private void markChunksUnused(int startChunk, int theChunkCount) {
		for (int i = 0; i < theChunkCount; i++) {
			chunkAllocateTrack.clear(startChunk + i);
		}
	}
	
	/**
	 * 统计page使用的容量
	 * @return
	 */
	public int size() {
		return chunkAllocateTrack.cardinality() * chunkSize;
	}
}
