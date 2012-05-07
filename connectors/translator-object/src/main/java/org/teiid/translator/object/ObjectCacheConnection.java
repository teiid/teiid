package org.teiid.translator.object;

import java.util.List;

/**
 * This is the interface the connection is exposed as.  
 * @author vhalbert
 *
 */
public interface ObjectCacheConnection {
	
	List<Object> get(List<Object> args, String cacheName, Class<?> rootNodeType) throws Exception ;

}
