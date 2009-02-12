/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */



package com.metamatrix.connector.xml;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.base.LoggingConnector;
import com.metamatrix.connector.xml.cache.DocumentCache;
import com.metamatrix.connector.xml.cache.IDocumentCache;
import com.metamatrix.connector.xml.cache.RequestRecord;

public abstract class AbstractCachingConnector extends LoggingConnector implements CachingConnector
{

   private IDocumentCache m_documentCache;

   private IDocumentCache m_statementCache;

   private Map requestInfo;

   public AbstractCachingConnector()
   {
      super();
   }

   public void start(ConnectorEnvironment env) throws ConnectorException
   {
     super.start(env);
     m_documentCache = new DocumentCache(m_state.getMaxMemoryCacheSizeByte(), m_state.getMaxFileCacheSizeByte(),
           m_state.getCacheLocation(), m_state.getCacheTimeoutMillis(), getLogger(), env.getConnectorName(), true);

     // It would be nice if there was a way to tell the cache that
     // nothing,
     // expires, but there is no way, so I just give it a very large
     // integer
     m_statementCache = new DocumentCache(Integer.MAX_VALUE / 2, 0, m_state.getCacheLocation(),
           Integer.MAX_VALUE / 2, getLogger(), env.getConnectorName() + "_STMT", false);//$NON-NLS-1$ 

     requestInfo = new HashMap();
   }

   /*
    * (non-Javadoc)
    * 
    * @see com.metamatrix.connector.xml.CachingConnector#getCache()
    */
   public IDocumentCache getCache()
   {
      return m_documentCache;
   }

   /*
    * (non-Javadoc)
    * 
    * @see com.metamatrix.connector.xml.CachingConnector#getStatementCache()
    */
   public IDocumentCache getStatementCache()
   {
      return m_statementCache;
   }

   public void stop()
   {
      if (m_documentCache != null)
      {
         getLogger().logTrace("Shutting down cache cleaner");//$NON-NLS-1$ 
         m_documentCache.shutdownCleaner();
      }
   }

   public void createCacheObjectRecord(String requestID, String partID, String executionID, String sourceRequestID, String cacheKey) throws ConnectorException
   {
      RequestRecord request = (RequestRecord) requestInfo.get(requestID);
      if (null == request)
      {
         request = new RequestRecord(requestID, this);
         requestInfo.put(requestID, request);
         getLogger().logTrace("Creating RequestRecord for Request Identifier " + requestID);//$NON-NLS-1$ 
         request.addRequestPart(partID, executionID, sourceRequestID, cacheKey, getLogger());
      }
      else
      {
         request.addRequestPart(partID, executionID, sourceRequestID, cacheKey, getLogger());
      }
   }

   public void deleteCacheItems(String requestID, String partID, String executionID)
   {
      RequestRecord request = (RequestRecord) requestInfo.get(requestID);
      if (null != request)
      {
         request.deleteRequestPart(partID, executionID, getLogger());
         if(request.isEmpty()) {
            requestInfo.remove(requestID);
            getLogger().logTrace("Removed RequestRecord for Request Identifier " + requestID);//$NON-NLS-1$ 
         }
         

      }
   }
}
