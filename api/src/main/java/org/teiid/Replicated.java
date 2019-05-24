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

package org.teiid;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to replicate Teiid components - this should be used in extension logic.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface Replicated {

    enum ReplicationMode {
        PUSH,
        PULL,
        NONE
    }

    /**
     * @return true if members should be called asynchronously.  asynch methods should be void.
     */
    boolean asynch() default true;
    /**
     * @return the timeout in milliseconds, or 0 if no timeout.  affects only synch calls.
     */
    long timeout() default 0;
    /**
     * @return true if only remote members should be called.  should not be used with replicateState.  method should be void.
     */
    boolean remoteOnly() default false;
    /**
     * Should not be used with remoteOnly.
     *
     * @return PUSH if the remote members should have a partial state replication called using the first argument as the state after
     *  the local method has been invoked, or PULL if the local member should initial a partial state pull using the first argument
     *  as the state after the local method returns null.  PULL cannot be asynch.
     */
    ReplicationMode replicateState() default ReplicationMode.NONE;

}