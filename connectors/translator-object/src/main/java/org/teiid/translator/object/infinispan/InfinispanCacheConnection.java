package org.teiid.translator.object.infinispan;

import java.util.List;

import org.teiid.translator.object.SearchCriterion;


public interface InfinispanCacheConnection  {
	
	List<Object> get(SearchCriterion criterion, String cacheName, Class<?> rootNodeType) throws Exception ;

}
