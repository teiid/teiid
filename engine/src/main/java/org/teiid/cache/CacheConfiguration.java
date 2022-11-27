/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
