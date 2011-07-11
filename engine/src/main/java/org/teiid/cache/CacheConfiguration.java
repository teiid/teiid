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

package org.teiid.cache;

import org.teiid.dqp.internal.process.SessionAwareCache;

public class CacheConfiguration {
			
	public enum Policy {
		LRU,  // Least Recently Used
		EXPIRATION
	}
	
	private Policy policy;
	private int maxage = -1;
	private int maxEntries = SessionAwareCache.DEFAULT_MAX_SIZE_TOTAL;
	private String name;
	private String location;
	
	private int maxStaleness = 0;
	
	public CacheConfiguration() {
	}
	
	public CacheConfiguration(Policy policy, int maxAgeInSeconds, int maxNodes, String location) {
		this.policy = policy;
		this.maxage = maxAgeInSeconds;
		this.maxEntries = maxNodes;
		this.location = location;
	}
	
	public Policy getPolicy() {
		return this.policy;
	}

	public int getMaxAgeInSeconds(){
		return maxage;
	}

	public void setMaxAgeInSeconds(int maxage){
		this.maxage = maxage;
	}
	
	public int getMaxStaleness() {
		return maxStaleness;
	}
	
	public void setMaxStaleness(int maxStaleDataModification) {
		this.maxStaleness = maxStaleDataModification;
	}
	
	public int getMaxEntries() {
		return this.maxEntries;
	}

	public void setMaxEntries(int entries) {
		this.maxEntries = entries;
	}

	public void setType (String type) {
		this.policy = Policy.valueOf(type);
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}	
}
