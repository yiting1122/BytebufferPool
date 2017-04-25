package com.yiting.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by hzyiting on 2017/4/25.
 */
public class DirectByteBufferPool implements BufferPool {
	private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufferPool.class);
	public static final String LOCAL_BUF_THREAD_PREX = "$_";
	private ByteBufferPage[] allPages;
	private final int chunkSize;
	private AtomicInteger prevAllocatePage;
	private final int pageSize;
	private final short pageCount;
	private final int conReadBufferChunk;
	
	/**
	 * 记录每个bufferpool中线程使用的情况
	 */
	private final ConcurrentHashMap<Long, Long> memoryUsage;
	
	public DirectByteBufferPool(int chunkSize, int pageSize, short pageCount, int conReadBufferChunk) {
		allPages = new ByteBufferPage[pageCount];
		this.chunkSize = chunkSize;
		this.pageSize = pageSize;
		this.pageCount = pageCount;
		this.conReadBufferChunk = conReadBufferChunk;
		prevAllocatePage = new AtomicInteger(0);
		for (int i = 0; i < pageCount; i++) {
			allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
		}
		this.memoryUsage = new ConcurrentHashMap<Long, Long>();
	}
	
	/**
	 * buffer 扩容
	 *
	 * @param buffer
	 * @return
	 */
	public ByteBuffer expandBuffer(ByteBuffer buffer) {
		int oldCapacity = buffer.capacity();
		int newCapacity = oldCapacity << 1;
		ByteBuffer newBuffer = allocate(newCapacity);
		if (newBuffer != null) {
			int newPosition = buffer.position();
			buffer.flip();
			newBuffer.put(buffer);
			newBuffer.position(newPosition);
			recycle(buffer);
			return newBuffer;
		}
		return null;
	}
	
	private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
		for (int i = startPage; i < endPage; i++) {
			ByteBuffer buffer = allPages[i].allockChunk(theChunkCount);
			if (buffer != null) {
				prevAllocatePage.getAndSet(i);
				return buffer;
			}
		}
		return null;
	}
	
	public ByteBuffer allocate(int size) {
		final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
		int selectPage = prevAllocatePage.incrementAndGet() % allPages.length;
		ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectPage);
		if (byteBuf == null) {
			byteBuf = allocateBuffer(theChunkCount, 0, allPages.length);
		}
		
		final long threadId = Thread.currentThread().getId();
		if (byteBuf != null) {
			if (memoryUsage.contains(threadId)) {
				memoryUsage.put(threadId, memoryUsage.get(threadId) + byteBuf.capacity());
			} else {
				memoryUsage.put(threadId, (long) byteBuf.capacity());
			}
			
		}
		if (byteBuf == null) {
			return ByteBuffer.allocate(size);
		}
		return byteBuf;
	}
	
	public void recycle(ByteBuffer theBuf) {
		if (theBuf != null && (!(theBuf instanceof DirectBuffer))) {
			theBuf.clear();
		}
		
		final long size = theBuf.capacity();
		boolean recycled = false;
		DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
		int chunkCount = theBuf.capacity() / chunkSize;
		DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
		int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / chunkSize);
		
		for(int i=0;i<allPages.length;i++){
			if((recycled = allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount))==true){
				break;
			}
		}
		
		final long threadId = Thread.currentThread().getId();
		
		if (memoryUsage.containsKey(threadId)) {
			memoryUsage.put(threadId, memoryUsage.get(threadId) - size);
		}
		if (recycled == false) {
			LOGGER.warn("warning ,not recycled buffer " + theBuf);
		}
	}
	
	public long capacity() {
		return 0;
	}
	
	public long size() {
		return 0;
	}
	
	public int getConReadBufferChunk() {
		return 0;
	}
	
	public int getSharedOptsCount() {
		return 0;
	}
	
	public int getChunkSize() {
		return 0;
	}
	
	public ConcurrentHashMap<Long, Long> getNetDirectMemoryUsage() {
		return null;
	}
	
	public BufferArray allocateArray() {
		return null;
	}
}
