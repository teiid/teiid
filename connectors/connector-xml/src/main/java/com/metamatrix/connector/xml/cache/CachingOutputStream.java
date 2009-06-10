package com.metamatrix.connector.xml.cache;

import java.io.IOException;
import java.io.OutputStream;

import org.teiid.connector.api.ExecutionContext;

public class CachingOutputStream extends OutputStream {

	private ExecutionContext context;
	private String requestId;
	private Integer chunkIndex;
	private boolean idCached = false;
	private StringBuffer buff;
	
	public CachingOutputStream(ExecutionContext context, String requestId) {
		super();
		this.context = context;
		this.requestId = requestId;
		chunkIndex = Integer.valueOf(0);
		buff = new StringBuffer();
	}
	
	@Override
	public void write(int b) throws IOException {
		buff.append((char)b);
		chunk();
	}
	
	
    @Override
	public void flush() throws IOException {
		super.flush();
		if(buff.length()!= 0) {
			context.put(requestId + chunkIndex.toString(), buff.toString());
			chunkIndex++;
			buff = new StringBuffer();
		}
	}

	@Override
	public void close() throws IOException {
		flush();
		super.close();
	}

	/**
     * 8192 * 5 / 16
     * 1kb * 5 / 16 bit chars = 2560 chars
     */
	private void chunk() {
		if(!idCached) {
			context.put(requestId, Boolean.TRUE);
			idCached = true;
		}
		
		if(buff.length() >= 2560) {
			context.put(requestId + chunkIndex.toString(), buff.toString());
			chunkIndex++;
			buff = new StringBuffer();
		}	
	}
	
	public CachedXMLStream getCachedXMLStream() {
		return new CachedXMLStream(context, requestId);
	}
}
