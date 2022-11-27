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
package org.teiid.events;

import org.teiid.adminapi.VDB;


/**
 * A listener interface than can be registered with {@link EventDistributor} that will notify
 * the events occurring in the Teiid engine
 */
public interface EventListener {

    /**
     * Invoked when VDB is deployed
     * @param vdbName
     * @param vdbVersion
     */
    void vdbDeployed(String vdbName, String vdbVersion);

    /**
     * Invoked when VDB undeployed
     * @param vdbName
     * @param vdbVersion
     */
    void vdbUndeployed(String vdbName, String vdbVersion);

    /**
     * VDB and all its metadata has been loaded and in ACTIVE state.
     * @param vdb
     */
    void vdbLoaded(VDB vdb);

    /**
     * VDB failed to load and in FAILED state; Note this can be called multiple times for given VDB
     * @param vdb
     */
    void vdbLoadFailed(VDB vdb);
}
