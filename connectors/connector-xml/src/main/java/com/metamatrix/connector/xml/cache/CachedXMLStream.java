package com.metamatrix.connector.xml.cache;

import java.io.IOException;
import java.io.InputStream;

import org.teiid.connector.api.ExecutionContext;


/**
 * 
 * Reassembles the Document stream that has been chunked into the cache.
 * We chunk it into the cache in the first pace because we don't want to
 * hold large documents in memory and we can't stream it into the cache.
 * 
 * The structure in the cache is the same across the different 
 * implementations, but semantically different.
 * 
 *   CacheRoot
 *   	ConnectorName
 *   		queryNode
 *   			chunks
 *   
 *   For the file connector, queryNode is one of the documents in
 *   directory specified in the connector binding.  For this 
 *   implementation the responses are not dynamic, so there is no
 *   need to group them by query.
 *   
 *   In the REST and SOAP implementations, the content of the xml 
 *   is presumed to be dynamic and determined by request to the source.
 *   In these cases the queryNode is a key generated from the request.
 *   
 *   The chunk level in the cache contains the chunked pieces of the 
 *   xml stream.
 *
 */
public class CachedXMLStream extends InputStream {

	private Integer keyIndex;
	private char[] chunk;
	private int chunkIndex = 0;
	private String requestId;
	private ExecutionContext context;

	public CachedXMLStream(ExecutionContext context, String requestId) {
		this.context = context;
		this.requestId = requestId;
		keyIndex = Integer.valueOf(0);
	}

	@Override
	public int read() throws IOException {
		int retval = -1;
		if(null == chunk || chunkIndex >= chunk.length) {
			String newChunk = (String) context.get(requestId + keyIndex.toString());
			if(null != newChunk) {
				chunk = newChunk.toCharArray();
				chunkIndex = 0;
				++keyIndex;
			} else {
				return retval;
			}
		}
		if(chunkIndex <= chunk.length) {
			retval = chunk[chunkIndex];
			chunkIndex++;
		}
		return retval;
	}

}
