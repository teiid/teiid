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

package org.teiid.adminapi;




/**
 * When a user submits a SQL command to the system for processing, usually that represents
 * a single request. A single request might have one or more source
 * requests (the requests that are being processed on the physical data sources) as part
 * of original request.
 *
 *  <p>A request is identified by a numbers separated by '|'. usually in they are arranged
 *  in the pattern [session]|[request] or [session]|[request]|[source request]
 */
public interface Request extends RequestBean, AdminObject, DomainAware {

    public enum ProcessingState {
        PROCESSING,
        DONE,
        CANCELED
    }

    public enum ThreadState {
        RUNNING,
        QUEUED,
        IDLE
    }

    /**
     * @return Returns whether this is a Source Request.
     */
    public boolean sourceRequest();

}
