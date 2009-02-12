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

package com.metamatrix.platform.security.authorization.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.cache.Cache;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * This class represents a local cache of access information, decreasing the
 * time required to determine a particular user's access privileges for specific
 * resources by maximizing the in-memory access and minimizing the calls
 * to the service provider's persistent store.
 * <p>
 */
public class AuthorizationCache {
    
	private Cache<AuthorizationPolicyID, AuthorizationPolicy> policyCache;
	private Cache<CacheKey, Collection> principalCache;
    
    private static class CacheKey implements Serializable {
        
        private static final long serialVersionUID = 3712007533668645365L;
        private MetaMatrixPrincipalName principal;
        private MetaMatrixSessionID sessionId;
        
        CacheKey(){}
        
        CacheKey(MetaMatrixPrincipalName principal, MetaMatrixSessionID sessionId) {
            this.principal = principal;
            this.sessionId = sessionId;
        }
        
        /** 
         * @see java.lang.Object#hashCode()
         */
        public int hashCode() {
            return principal.hashCode();
        }
        
        /** 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            
            if (!(obj instanceof CacheKey)) {
                return false;
            }
            
            CacheKey other = (CacheKey)obj;
            
            if (!this.principal.equals(other.principal)) {
                return false;
            }
            
            if (this.sessionId == null || other.sessionId == null) {
                return true;
            }
            
            return this.sessionId.equals(other.sessionId);
        }
        
    }

    /**
     * Construct with cache properties.
     * @param policyCacheName The name that will be assigned to the policy cache for this
     * Authorization service instance.
     * @param principalCacheName The name that will be assigned to the principal cache for this
     * Authorization service instance.
     * @param env The cache properties - may be null.
     * @throws ObjectCacheException if cache properties are incorrect.
     */
    public AuthorizationCache(Cache policyCache, Cache principalCache, Properties env ) {
    	this.policyCache = policyCache;
    	this.principalCache = principalCache;
    }

    /**
     * Find the policy with the given ID.
     * @param policyID The ID of the policy looking for.
     * @return The policy or <code>null</code> if none with given ID has been cached.
     */
    public synchronized AuthorizationPolicy findPolicy( AuthorizationPolicyID policyID ) {
        AuthorizationPolicy policy;
        
        policy = this.policyCache.get(policyID);
        return policy;
    }
    
    /**
     * Find the policies with the given IDs.
     * @param policyIDs The collection of poilicyIDs for the policies looking for.
     * @return The collection of policies with the given IDs or an empty collection
     * - never null.
     */
    public synchronized Collection findPolicies( final Collection policyIDs ) {
        final Collection policies = new ArrayList();
        Iterator idItr = policyIDs.iterator();
        while ( idItr.hasNext() ) {
            final Object aPolicy =  this.policyCache.get((AuthorizationPolicyID)idItr.next());
            if (aPolicy != null) {
                policies.add(aPolicy);
            }
        }
        return policies;
    }

    /**
     * Find the policyIDs associated with the given principal.
     * @param userName The user name of the principal.
     * @return The collection of PolicyIDs associated with given principal or an empty collection
     * - never null.
     */
    public synchronized Collection findPolicyIDs( final MetaMatrixPrincipalName user, SessionToken session ) {
        Collection policyIDs = (Collection) this.principalCache.get(createCacheKey(user, session));
        if (policyIDs == null) {
            return new ArrayList();
        }
        return new ArrayList(policyIDs);
    }

    /**
     * Remove from the cache any policy IDs referenced by the specified principal.
     * This method does <i>not</i> remove from the cache any of the referenced policy objects.
     * @param userName The user name of the principal.
     */
    public synchronized void removePrincipalFromCache( final MetaMatrixPrincipalName user ) {
        CacheKey key = createCacheKey(user, null);
        
        Set<CacheKey> cachedPrincipals = this.principalCache.keySet();
        while (cachedPrincipals.contains(key)) {
            this.principalCache.remove(key);
        }
    }

    /**
     * Remove from the cache all accounts and the policyIDs they reference.  This method does <i>not</i>
     * remove from the cache any of the referenced policy objects.
     */
    public synchronized void clearPrincipalsFromCache() {
    	this.principalCache.clear();
    }

    /**
     * Return the size of the Principal cache.
     * @return the size of the Principal cache.
     */
    public synchronized int principalCacheSize() {
    	return this.principalCache.size();
    }

    /**
     * Remove from the cache the policy with the specified ID.  This method does
     * <i>not</i> remove from the cache any accounts that reference this policy.
     * @param policyID the ID of the policy
     */
    public synchronized void removePolicyFromCache( final AuthorizationPolicyID policyID ) {
    	this.policyCache.remove(policyID);
    }

    /**
     * Remove from the cache the policies with the specified IDs.  This method does
     * <i>not</i> remove from the cache any accounts that reference this policy.
     * @param policyID the ID of the policy
     */
    public synchronized void removePolicysFromCache( final Collection policyIDs ) {
        if ( policyIDs != null && policyIDs.size() > 0 ) {
            Iterator policyIDItr = policyIDs.iterator();
            while ( policyIDItr.hasNext() ) {
            	this.policyCache.remove((AuthorizationPolicyID)policyIDItr.next() );
            }
        }
    }

    /**
     * Remove from the cache all policies.  This method does <i>not</i>
     * remove from the cache any accounts that reference this policy.
     */
    public synchronized void clearPoliciesFromCache() {
    	this.policyCache.clear();
    }

    /**
     * Return the size of the policy cache.
     * @return the size of the policy cache.
     */
    public synchronized int policyCacheSize() {
    	return this.policyCache.size();
    }

    /**
     * Get the collection of policy IDs that are part of this collection but are
     * not found in the cache.
     * @param sessionToken The session token of the principal.
     * @return The collection of PolicyIDs that are a subset of given policy ID
     * collection but are not cached.
     */
    public synchronized Collection getPolicyIDsNotCached( final Collection policyIDs ) {
    	HashSet absentPolicyIDs = new HashSet(policyIDs);
    	absentPolicyIDs.removeAll(this.policyCache.keySet());
    	
        return absentPolicyIDs;
    }

    /**
     * Clear both Authorization caches (principal cache and policy cache).
     * <br>Use this method when an AuthorizationPolicy has been modified, which
     * should be infrequent in a stable system.</br>
     * <p>
     * Since the authorization cache is distributed, a clear event will be
     * broadcast to all AuthorizationCaches in the system.<p>
     */
    public synchronized void clearCaches() {
    	clearPoliciesFromCache();
    	clearPrincipalsFromCache();
    }

    /**
     * Load the principal and the policyIDs that apply to him.
     * <p><i>Note</i>: Any policyIDs for this user that were
     * previously in the cache <i>are not</i> removed - the new
     * policyIDs are added to them.</p>
     * @param userName the user name principal for which the applicable policyIDs
     * are to be loaded.
     */
    public synchronized void cachePolicyIDsForPrincipal( final MetaMatrixPrincipalName userName, SessionToken session,
                                            final Collection policyIDs ) {
        final Collection policyIDsCopy = new ArrayList(policyIDs);
        // Associate these IDs with the principal (and cache this association) ...
        final Collection prevIDs = this.findPolicyIDs(userName, session);
        if ( prevIDs != null && prevIDs.size() > 0 ) {
            policyIDsCopy.addAll(prevIDs);
        }
        this.principalCache.put(createCacheKey(userName, session), policyIDsCopy);
    }

    private CacheKey createCacheKey(final MetaMatrixPrincipalName user, SessionToken session) {
        return new CacheKey(user, session!=null?session.getSessionID():null);
    }


    /**
     * Cache the policies that have the specified IDs.  Callers should ensure that
     * all policies given are not already cached.
     * @param policies The policies that are to be loaded.
     */
    public synchronized void cachePoliciesWithIDs( final Collection policies ) {
        Iterator iter = policies.iterator();
        while ( iter.hasNext() ) {
            final AuthorizationPolicy policy = (AuthorizationPolicy) iter.next();
            this.policyCache.put(policy.getAuthorizationPolicyID(), policy );
        }
    }

    /**
     * Cache the policy with the specified ID.  If the policy is already cached,
     * it is not reloaded.
     * @param policyIDs The IDs of the policies that are to be loaded.
     */
    public synchronized void cachePolicyWithID( final AuthorizationPolicy policy ) {
        // Cache any new policy IDs and policies ...
    	this.policyCache.put(policy.getAuthorizationPolicyID(), policy );
    }

}
