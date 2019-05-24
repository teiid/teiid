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

public interface WorkerPoolStatisticsBean {

    /**
     * Current active thread count
     * @return
     */
    public int getActiveThreads();

    /**
     * Highest Active threads recorded so far
     * @return
     */
    public int getHighestActiveThreads();


     /**
      * Queue Name
      * @return
      */
     public String getQueueName();


     /**
      * Max number of active threads allowed
      * @return
      */
     public int getMaxThreads();

    /**
     * @return Returns the number of requests queued.
     * @since 4.3
     */
    public int getQueued();

    /**
     * @return The number of completed tasks
     */
    long getTotalCompleted();


    /**
     * @return The number of submitted tasks
     */
    long getTotalSubmitted();

    /**
     * @return Returns the highest queue size
     */
    public int getHighestQueued();

}
