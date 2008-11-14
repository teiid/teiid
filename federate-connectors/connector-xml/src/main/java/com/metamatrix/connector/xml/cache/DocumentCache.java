/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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



package com.metamatrix.connector.xml.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.metamatrix.connector.xml.base.XMLDocument;
import com.metamatrix.data.api.ConnectorLogger;

/**
 * 
 * created by JChoate on Jun 22, 2005
 * 
 * Provides an in memory object cache backed by a file cache. Objects entering
 * the cache should be Serializable if the file cache is going to be used. If
 * they are not serializable, the will not be written to disk, they will just be
 * discarded if they are removed from the in-memory cache.
 * 
 * The cache should be thread safe and usable by any connector (or really
 * anything)
 * 
 * 
 */

public class DocumentCache implements IDocumentCache {

    private int m_maxTimeToLive;

    private int m_maxMemoryCacheSize;

    private int m_maxFileCacheSize;

    private int m_currentMemoryCacheSize;

    private int m_currentFileCacheSize;

    private Hashtable m_cacheImpl;
    
    private List deleteList;

    private CachedObject m_mostRecentUsed;

    private CachedObject m_leastRecentUsed;

    private CacheCleaner m_cleaner;

    private String m_fileCacheLocation;

    private ConnectorLogger m_log;

    private String m_cacheIdentifier;

    private int m_memoryCacheCount = 0;

    private int m_fileCacheCount = 0;

	private boolean m_performanceCache = false;
    
    CachedObjectRemover cachedObjectRemover;

    private static final int UNLIMITED_CACHE_THRESHOLD = -1;

    public DocumentCache(int maxMemoryCacheSize, int maxFileCacheSize,
            String fileCacheLocation, int maxTimeToLive, ConnectorLogger log,
            String identifier, boolean useCleanerThread) {
        m_log = log;
        if(maxTimeToLive != 0) {
           m_performanceCache = true;
           m_cleaner = new CacheCleaner(useCleanerThread);
           m_log.logTrace("Performance File cache is enabled");
        } else {
           m_log.logTrace("Performance File cache is disabled");
        }
        m_maxMemoryCacheSize = maxMemoryCacheSize;
        m_log.logTrace("File cache Max Memory Cache Size " + maxMemoryCacheSize);
        m_maxTimeToLive = maxTimeToLive;
        m_log.logTrace("File cache Max Time to Live " + m_maxTimeToLive);
        m_maxFileCacheSize = maxFileCacheSize;
        m_log.logTrace("File cache location is " + m_fileCacheLocation);
        m_fileCacheLocation = fileCacheLocation;
        m_currentMemoryCacheSize = 0;
        m_currentFileCacheSize = 0;
        m_cacheImpl = new Hashtable();
        m_mostRecentUsed = null;
        m_leastRecentUsed = m_mostRecentUsed;
        m_cacheIdentifier = identifier;
        deleteList = new ArrayList();
        cachedObjectRemover = new CachedObjectRemover();
        cachedObjectRemover.start();
    }

    public DocumentCache(int maxMemoryCacheSize, int maxFileCacheSize,
            String fileCacheLocation, int maxTimeToLive, ConnectorLogger log,
            String identifier) {
        this(maxMemoryCacheSize, maxFileCacheSize, fileCacheLocation,
                maxTimeToLive, log, identifier, true);
    }

    public static XMLDocument cacheLookup(IDocumentCache cache, String cacheKey,
            String id) {
        XMLDocument doc = null;
        Object tmpObject = cache.fetchObject(cacheKey, id);
        if (tmpObject != null) {
            doc = (XMLDocument) tmpObject;
        }
        return doc;
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.cache.IDocumentCache#addToCache(java.lang.String, java.lang.Object, int, java.lang.String, java.lang.String)
	 */
    public synchronized void addToCache(String cacheKey, Object obj, int size, 
    		String id) {

        CachedObject newItem;
        // test for conditions where object cannot be cached
        m_log
                .logTrace("Attempting to cache item identified by " + cacheKey + "; estimated memory size is " + size + " bytes."); //$NON-NLS-1$
        if (((m_maxMemoryCacheSize > UNLIMITED_CACHE_THRESHOLD && size > m_maxMemoryCacheSize) && (m_maxFileCacheSize > UNLIMITED_CACHE_THRESHOLD && size > m_maxFileCacheSize))) {
            m_log.logTrace(m_cacheIdentifier
                    + ": unable to cache item identified by " + cacheKey); //$NON-NLS-1$
            return;
        }

        newItem = new CachedObject();
        if (m_performanceCache) 
        {
           newItem.setExpires(System.currentTimeMillis() + m_maxTimeToLive);
        }
        newItem.setItemID(cacheKey);
        newItem.setPayload(obj);
        newItem.setSize(size);
        newItem.addReference(id);
        if (obj instanceof EventSinkFactory) {
            EventSink eventSink = ((EventSinkFactory) obj).getEventSink();
            newItem.setEventSink(eventSink);
        }

        // is the cache empty?
        if (m_currentMemoryCacheSize == 0 && m_currentFileCacheSize == 0) {
            // its the first one
            // set as least recent
            m_leastRecentUsed = newItem;

        } else if (m_cacheImpl.get(cacheKey) != null) {
            // replace the existing one
            remove((CachedObject) m_cacheImpl.get(cacheKey));
        }
        if (size <= m_maxMemoryCacheSize
                || m_maxMemoryCacheSize <= UNLIMITED_CACHE_THRESHOLD) {
            addItemToMemoryCache(newItem);
        } else {
            addItemToFileCache(newItem);
        }

        // not the first item
        if (m_mostRecentUsed != null) {
            m_mostRecentUsed.setPrevious(newItem);
        }

        // set as most recent
        m_mostRecentUsed = newItem;

        // add it to the hash
        m_cacheImpl.put(newItem.getItemId(), newItem);
        m_log.logTrace(m_cacheIdentifier
                + ": adding item to cache as " + newItem.getItemId()); //$NON-NLS-1$
        logCacheReport();

        // add it to the cleanup list
        if(m_performanceCache)
        {
           m_cleaner.scheduleRemoval(newItem);
        }
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.cache.IDocumentCache#release(java.lang.String, java.lang.String, java.lang.String)
	 */
    public synchronized void release(String cacheKey, String id) {
    	CachedObject cacheObject = (CachedObject)m_cacheImpl.get(cacheKey);
    	if(null != cacheObject) { 
	    	cacheObject.removeReference(id);
	        m_log.logTrace("Removing reference to Request Identification " + id
	               + " from cache item " + cacheKey);
	        
	        //If performanceCache is on, then the cache cleaner will clean up.
	    	//if its off, we clean up here.
	    	if(!m_performanceCache && !cacheObject.isLocked()) {
	           m_log.logTrace("Moving cache item " + cacheKey + " to the delete list");
	            removeLink(cacheObject);
	    		m_cacheImpl.remove(cacheKey);
	    		cacheObject.setListed(false);
	    		synchronized(deleteList) {
	    			deleteList.add(cacheObject);
	    		}
	    	}
    	}
    }
    
    private void remove(CachedObject ref) {
        removeLink(ref);
        m_cacheImpl.remove(ref.getItemId());
        m_log
                .logTrace(m_cacheIdentifier
                        + ": removing item from cache identified by : " + ref.getItemId()); //$NON-NLS-1$

        // manage cache sizes and remove files
        if (ref.getPayload() instanceof FileCacheItem) {

            FileCacheItem item = (FileCacheItem) ref.getPayload();
            File itemFile = new File(item.getCacheFile());
            boolean success = itemFile.delete();
            if (success) {
                m_log.logTrace(m_cacheIdentifier
                        + ": removed file " + itemFile.getAbsolutePath() //$NON-NLS-1$  
                        + " from file cache"); //$NON-NLS-1$
            } else {
                m_log
                        .logError(m_cacheIdentifier
                                + ": failed to remove file " + itemFile.getAbsolutePath() + " from the cache"); //$NON-NLS-1$ //$NON-NLS-2$
            }
            decrementFileCacheCount();
            updateFileCacheSize(-ref.getSize());
        } else {
            decrementMemoryCacheCount();
            updateMemoryCacheSize(-ref.getSize());
        }

        if (ref.getEventSink() != null) {
            ref.getEventSink().onDelete();
        }
        m_log.logTrace(m_cacheIdentifier
                + ": removed item " + ref.getItemId() + " from cache"); //$NON-NLS-1$ //$NON-NLS-2$
        logCacheReport();
    }

    private void removeLink(CachedObject remove) {
        if (m_mostRecentUsed == remove) {
            m_mostRecentUsed = remove.getNext();
        }
        if (m_leastRecentUsed == remove) {
            m_leastRecentUsed = remove.getPrevious();
        }

        if (remove.getNext() != null) {
            remove.getNext().setPrevious(remove.getPrevious());
        }

        if (remove.getPrevious() != null) {
            remove.getPrevious().setNext(remove.getNext());
        }
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.cache.IDocumentCache#fetchObject(java.lang.String, java.lang.String, java.lang.String)
	 */
    public synchronized Object fetchObject(String cacheKey, String id) {
       m_log.logTrace("Looking up " + cacheKey + " | " + id); 
       CachedObject foundLink = (CachedObject) m_cacheImpl.get(cacheKey);
        if (foundLink == null) {
            m_log.logTrace(m_cacheIdentifier
                    + ": could not find item in cache with id " + cacheKey); //$NON-NLS-1$
            return null;
        }
        
        String message = m_cacheIdentifier + ": found item in cache with id " + cacheKey;
        if(foundLink.isExpired()) {
        	if(foundLink.hasReference(id)) {
        		foundLink.addReference(id);
        	} else {
        		message += ", Item is expired, returning null";
        		m_log.logTrace(message);
        		return null;
        	}
        }
        m_log.logTrace(message);
        
        // Don't move it to most recent, this coould possible cause a cache object
        // to remain in the cache indefinitely
/*        if (m_mostRecentUsed != foundLink) {
            // move it to most recent
            if (foundLink == m_leastRecentUsed
                    && foundLink.getPrevious() != null) {
                m_leastRecentUsed = foundLink.getPrevious();
                m_leastRecentUsed.setNext(null);
            }

            // remove from linked list
            removeLink(foundLink);

            // reenter list at head
            m_mostRecentUsed.setPrevious(foundLink);
            foundLink.setPrevious(null);
            foundLink.setNext(m_mostRecentUsed);
            m_mostRecentUsed = foundLink;
        }
*/        
        Object o = null;
        if (foundLink.getPayload() instanceof FileCacheItem) {
            o = deserializeFileCacheItem((FileCacheItem) foundLink.getPayload());
            if (foundLink.getEventSink() != null) {
                foundLink.getEventSink().onRestoreFromFile(o);
            }
            updateFileCacheSize(-foundLink.getSize());
            if (o == null) {
                // the file cache is bad - pretend its not there
            	m_cacheImpl.remove(foundLink.getItemId());
            }
        } else {
            o = foundLink.getPayload();
        }
        foundLink.addReference(id);
        return o;
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.cache.IDocumentCache#clearCache()
	 */
    public synchronized void clearCache() {
        m_cleaner.interrupt();
        m_cleaner.clearCache();
        cachedObjectRemover.interrupt();
    }

    private void addItemToFileCache(CachedObject item) {
        if (m_maxFileCacheSize > UNLIMITED_CACHE_THRESHOLD
                && item.getSize() > m_maxFileCacheSize) {
            m_log
                    .logTrace(item.getItemId()
                            + " cannot be cached to file.  Cache size would be exceeded");
            return;
        }
        final String extension = ".ser"; //$NON-NLS-1$
        if (item.getPayload() instanceof Serializable) {
            File serialized = new File(m_fileCacheLocation + File.separator
                    + item.hashCode() + extension);
            try {
                ObjectOutputStream oos = new ObjectOutputStream(
                        new FileOutputStream(serialized));
                oos.writeObject(item.getPayload());
                oos.flush();
                oos.close();
            } catch (IOException e) {
                m_log.logTrace("IOException serializing " + item.getItemId()
                        + " Exception Message = " + e.getMessage());
                remove(item);
                return;
            }
            // make room
            while (m_maxFileCacheSize > UNLIMITED_CACHE_THRESHOLD
                    && m_currentFileCacheSize + item.getSize() >= m_maxFileCacheSize) {
                CachedObject obj = m_cleaner.m_head.getCurrent();
                // TODO: This may be a threading problem.
                m_cleaner.m_head = m_cleaner.m_head.getNext();
                m_log.logTrace("Removing file cache item " + obj.getItemId()
                        + " to make room for " + item.getItemId());
                m_log.logTrace("Consider increasing the memory cache size");
                remove(obj);
            }

            // add it
            FileCacheItem cacheItem = new FileCacheItem();
            cacheItem.setCacheFile(m_fileCacheLocation + File.separator
                    + item.hashCode() + extension);
            item.setPayload(cacheItem);
            incrementFileCacheCount();
            updateFileCacheSize(item.getSize());
            m_log.logTrace(m_cacheIdentifier
                 + ": added item " + item.getItemId() + " to file cache with name " + cacheItem.getCacheFile()); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            m_log.logTrace(item.getItemId()
                 + " cannot be cached to file.  It does not implement Serializable");
            remove(item);
        }
    }

    private void incrementFileCacheCount() {
        m_fileCacheCount++;
    }

    private void decrementFileCacheCount() {
        m_fileCacheCount--;
    }

    private void incrementMemoryCacheCount() {
        m_memoryCacheCount++;
    }

    private void decrementMemoryCacheCount() {
        m_memoryCacheCount--;
    }

    private void addItemToMemoryCache(CachedObject newItem) {
        final int writeTimeBuffer = 5000;
        // ensure there is room in memory cache
        while (m_maxMemoryCacheSize > UNLIMITED_CACHE_THRESHOLD
                && (m_currentMemoryCacheSize + newItem.getSize()) > m_maxMemoryCacheSize) {
            // if it expires soon, dump it now - it would expire by the time its
            // written
            if (m_leastRecentUsed != null && m_performanceCache
                    && m_leastRecentUsed.getExpires() <= (writeTimeBuffer + System
                            .currentTimeMillis())) {

                // remove from lookup
                m_cacheImpl.remove(m_leastRecentUsed.getItemId());
                // reduce current size
                updateMemoryCacheSize(-m_leastRecentUsed.getSize());
                // reset least recent to next and free object for gc
                m_leastRecentUsed = m_leastRecentUsed.getNext();

            } else {
                // find something that can be moved
                CachedObject removeCandidate = m_leastRecentUsed;
                while (removeCandidate != null
                        && removeCandidate.m_payload instanceof FileCacheItem) {
                    removeCandidate = removeCandidate.getPrevious();
                }
                if (removeCandidate != null) {
                    m_log.logTrace("Moving item " + removeCandidate.getItemId()
                            + " from memory to file cache");
                    addItemToFileCache(removeCandidate);
                    updateMemoryCacheSize(-removeCandidate.getSize());
                    decrementMemoryCacheCount();
                }
            }
        }

        // things look good so add the item

        // pick up the current most recent
        newItem.setNext(m_mostRecentUsed);
        newItem.setPrevious(null);

        incrementMemoryCacheCount();
        // increase the current cache size
        updateMemoryCacheSize(newItem.getSize());

    }

    private void updateMemoryCacheSize(int amount) {
        m_currentMemoryCacheSize += amount;
        if (m_currentMemoryCacheSize < 0) {
            m_currentMemoryCacheSize = 0;
        }
    }

    private void updateFileCacheSize(int amount) {
        m_currentFileCacheSize += amount;
        if (m_currentFileCacheSize < 0) {
            m_currentFileCacheSize = 0;
        }
    }

    private void logCacheReport() {
        if (m_maxMemoryCacheSize <= UNLIMITED_CACHE_THRESHOLD) {
            m_log.logTrace(m_cacheIdentifier
                    + " in-memory cache is unlimited. current usage: "
                    + getCurrentMemoryCacheSize());
        } else {
            m_log
                    .logTrace(m_cacheIdentifier
                            + ": remaining in-memory cache: " + (m_maxMemoryCacheSize - getCurrentMemoryCacheSize())); //$NON-NLS-1$
        }
        if (m_maxFileCacheSize <= UNLIMITED_CACHE_THRESHOLD) {
            m_log.logTrace(m_cacheIdentifier
                    + " file cache is unlimited. current usage: "
                    + getCurrentFileCacheSize());
        } else {
            m_log
                    .logTrace(m_cacheIdentifier
                            + ": remianing file memory cache: " + (m_maxFileCacheSize - getCurrentFileCacheSize())); //$NON-NLS-1$
        }
        m_log.logTrace("Count of items in memory cache: " + m_memoryCacheCount);
        m_log.logTrace("Count of items in file cache: " + m_fileCacheCount);
    }

    private Object deserializeFileCacheItem(FileCacheItem item) {
        Object obj;
        File cacheFile = new File(item.getCacheFile());
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new FileInputStream(cacheFile));
            obj = ois.readObject();
            ois.close();
            ois = null;
        } catch (Exception e) {
            obj = null;
        } finally {
            if (null != ois) {
                try {
                    ois.close();
                } catch (IOException e) {
                }
            }
        }
        return obj;
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.cache.IDocumentCache#shutdownCleaner()
	 */
    public void shutdownCleaner() {
        m_log.logTrace(m_cacheIdentifier + ": shutting down"); //$NON-NLS-1$
        if(m_performanceCache) {
           m_cleaner.interrupt();
        }
    }

    /* For testing purposes */

    protected int getCurrentMemoryCacheSize() {
        return m_currentMemoryCacheSize;
    }

    protected int getCurrentFileCacheSize() {
        return m_currentFileCacheSize;
    }

    protected int getCacheCount() {
        return m_cacheImpl.size();
    }

    protected Enumeration getCacheContents() {
        return m_cacheImpl.elements();
    }

    protected void dumpCache() {
        Enumeration enumer = getCacheContents();
        Vector memory = new Vector();
        Vector files = new Vector();

        while (enumer.hasMoreElements()) {
            CachedObject next = (CachedObject) enumer.nextElement();
            if (next.m_payload instanceof FileCacheItem) {
                files.add(next);
            } else {
                memory.add(next);
            }
        }

        m_log.logTrace(m_cacheIdentifier + ": memory cache items:"); //$NON-NLS-1$
        for (int i = 0; i < memory.size(); i++) {
            CachedObject obj = (CachedObject) memory.get(i);
            m_log.logTrace(obj.toString());
        }
        m_log.logTrace(m_cacheIdentifier + ": file cache items:"); //$NON-NLS-1$
        for (int i = 0; i < files.size(); i++) {
            CachedObject obj = (CachedObject) files.get(i);
            m_log.logTrace(obj.toString());
        }

        m_log.logTrace("most recent used item: "
                + ((m_mostRecentUsed == null) ? "null"
                        : m_mostRecentUsed.m_itemID));
        m_log.logTrace("least recent used item: "
                + ((m_leastRecentUsed == null) ? "null"
                        : m_leastRecentUsed.m_itemID));

    }

    /* for testing purposes */

    private class CachedObjectQueueItem {

        private CachedObjectQueueItem() {

        }

        private CachedObject m_current;

        private CachedObjectQueueItem m_next;

        private synchronized void setCurrent(CachedObject obj) {
            m_current = obj;
        }

        private synchronized CachedObject getCurrent() {
            return m_current;
        }

        private synchronized void setNext(CachedObjectQueueItem item) {
            m_next = item;
        }

        private synchronized CachedObjectQueueItem getNext() {
            return m_next;
        }

    }

    private class CacheCleaner extends Thread {

        private CachedObjectQueueItem m_head;

        private CachedObjectQueueItem m_tail;

        public CacheCleaner() {
            this(true);
        }

        public CacheCleaner(boolean start) {
            m_head = null;
            m_tail = null;
            if (start) {
                start();
            }
        }

        public void scheduleRemoval(CachedObject node) {
            CachedObjectQueueItem newItem = new CachedObjectQueueItem();
            newItem.setCurrent(node);
            newItem.setNext(null);
            synchronized (this) {
                if (m_head == null) {
                    m_head = newItem;
                }
                if (m_tail != null) {
                    m_tail.setNext(newItem);
                }
                m_tail = newItem;
            }
        }

        public void run() {
            long now;

            try {
                while (!isInterrupted()) {
                    while (m_head != null) {
                        now = System.currentTimeMillis();
                        long nextExpiration = m_head.getCurrent().getExpires();
                        if (now < nextExpiration) {
                            synchronized (this) {
                                m_log
                                        .logTrace("CacheCleaner waiting "
                                                + (nextExpiration - now)
                                                + " for expiration of item identified by: "
                                                + m_head.getCurrent()
                                                        .getItemId());
                                wait(nextExpiration - now);
                            }
                        }

                        synchronized (this) {
                            CachedObject removal = m_head.getCurrent();
                            m_head = m_head.getNext();
                            // is the cache empty?
                            // if this was the last entry, tail needs to be set
                            // to null
                            if (m_head == null) {
                                m_tail = null;
                            }
                            
                            if(removal.isLocked())
                            {
                               removeLink(removal);
                               removal.setListed(false);
                               deleteList.add(removal);
                            }
                               
                            else if (m_cacheImpl.get(removal.getItemId()) != null) 
                            {
                                m_log.logTrace("CacheCleaner removing "
                                        + removal.getItemId());
                                remove(removal);
                            } 
                            else 
                            { //for some reason the payload is already gone.
                                m_log.logTrace("CacheCleaner cannot remove item, ItemId is: " + removal.getItemId());
                                m_log.logTrace("Payload class is: "
                                        + removal.getPayload().getClass());
                                if (removal.getPayload() != null
                                        && removal.getPayload() instanceof FileCacheItem) {
                                    FileCacheItem item = (FileCacheItem) removal
                                            .getPayload();
                                    m_log.logTrace("FileCacheItem file is "
                                            + item.getCacheFile());
                                }
                            }
                        }
                    }
                    m_log.logTrace("CacheCleaner sleeping for "
                            + m_maxTimeToLive);
                    synchronized (this) {
                        wait(m_maxTimeToLive);
                    }
                }
            } catch (InterruptedException ie) {
                m_log.logTrace("CacheCleaner shutting down");
                clearCache();
                return;
            }

        }

        private synchronized void clearCache() {
            m_log.logTrace("CacheCleaner clearing the cache");
            while (m_head != null) {
                CachedObject done = m_head.getCurrent();
                m_head = m_head.getNext();
                remove(done);
            }
            m_log.logTrace("CacheCleaner cache cleared");
        }

    }

    private class CachedObjectRemover extends Thread {

      public void run()
      {
    	  try {
              while (!isInterrupted()) {
		    	  sleep(10000);
            	  synchronized(deleteList) {
		            List lockedObjects = new ArrayList(); 
		            Iterator iter = deleteList.iterator();
		            while(iter.hasNext()) {
		               CachedObject cachedObject = (CachedObject) iter.next();
		               m_log.logTrace("CachedObjectRemover removing " + cachedObject.m_itemID );
		               if(cachedObject.isLocked()) {
		            	   m_log.logTrace("CachedObjectRemover moving " + cachedObject.m_itemID + "to the locked objects list" );
		                  lockedObjects.add(cachedObject);
		               } else {
		                  remove(cachedObject);
		                  m_log.logTrace("CachedObjectRemover removed " + cachedObject.m_itemID);
		               }
		            }
		            deleteList = lockedObjects;
		         }
              }
    	  } catch (InterruptedException i) {
    		  m_log.logTrace("CachedObjectRemover shutting down");
    		  synchronized(deleteList) {
    			  Iterator iter = deleteList.iterator();
    			  while(iter.hasNext()) {
		               CachedObject cachedObject = (CachedObject) iter.next();
		               remove(cachedObject);
		               m_log.logTrace("CachedObjectRemover removed " + cachedObject.m_itemID);
    			  }
    		  }
    	  }
      }
    }
    
    public interface EventSink {
        void onDelete();

        void onRestoreFromFile(Object o);
    }

    public interface EventSinkFactory {
        EventSink getEventSink();
    }

    public class CachedObject implements Serializable {

        public static final long serialVersionUID = 1L;

        private Object m_payload;

        private String m_itemID;

        private int m_size;

        private long m_expires;

        private CachedObject m_previous;

        private CachedObject m_next;

        private EventSink m_eventSink;
        
        private boolean locked = false;
        
        /**
         * Map of Request Identifiers to Sets of Request Part Identifiers.
         * Used to reference count the CachedObject.
         */
        private Set references = new HashSet();
        
        /**
         * Indicates if the CachedObject is listed in the cache lookup Map.
         */
        private boolean listed = true;
        
        private CachedObject() {
        	
        }

        public boolean hasReference(String requestID) {
			return references.contains(requestID);
			}
			
		public void removeReference(String id) {
			references.remove(id);
			//If there are no more request references, then we can unlock this for a delete.
			if(references.isEmpty()) {
				setLocked(false);
				m_log.logTrace("unlocking cache item " + this.m_itemID);
			}
		}
        
        public void addReference(String id) {
			references.add(id);
        		setLocked(true);
        	}
        
		public boolean isLocked() {
			return locked;
		}
        
        public void setLocked(boolean lock) {
        	this.locked = lock;
        }

		private CachedObject(String id, Object payload) {
            m_itemID = id;
            m_payload = payload;
        }

        private synchronized void setPayload(Object payload) {
            m_payload = payload;
        }

        private synchronized Object getPayload() {
            return m_payload;
        }

        private void setItemID(String id) {
            m_itemID = id;
        }

        private String getItemId() {
            return m_itemID;
        }

        private synchronized void setSize(int size) {
            m_size = size;
        }

        private synchronized int getSize() {
            return m_size;
        }

        private synchronized void setExpires(long expires) {
            m_expires = expires;
        }

        private synchronized long getExpires() {
            return m_expires;
        }
        
        public boolean isExpired() {
        	boolean result = true;
        	if (System.currentTimeMillis() < getExpires()) {
        		result = false;
        	}
        	return result;
        }

        private synchronized void setPrevious(CachedObject previous) {
            m_previous = previous;
        }

        private synchronized CachedObject getPrevious() {
            return m_previous;
        }

        private synchronized void setNext(CachedObject next) {
            m_next = next;
        }

        private synchronized CachedObject getNext() {
            return m_next;
        }

        private void setEventSink(EventSink eventSink) {
            this.m_eventSink = eventSink;
        }

        private EventSink getEventSink() {
            return m_eventSink;
        }

        public String toString() {
            return new String("id: " + getItemId() + " size: " + getSize() //$NON-NLS-1$ //$NON-NLS-2$
                    + " expires: " + getExpires()); //$NON-NLS-1$
        }

		public boolean isListed() {
			return listed;
		}

		public void setListed(boolean listed) {
			this.listed = listed;
		}
    }

    private class FileCacheItem {

        private FileCacheItem() {

        }

        private String m_cacheFile;

        private synchronized void setCacheFile(String file) {
            m_cacheFile = file;
        }

        private synchronized String getCacheFile() {
            return m_cacheFile;
        }
    }

}
