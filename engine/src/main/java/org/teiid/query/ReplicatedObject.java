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

package org.teiid.query;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;

/**
 * Optional interface to be implemented by a replicated object to support full and partial state transfer.
 *
 */
public interface ReplicatedObject<K extends Serializable> {

    /**
     * Allows an application to write a state through a provided OutputStream.
     *
     * @param ostream the OutputStream
     */
    void getState(OutputStream ostream);

    /**
     * Allows an application to write a partial state through a provided OutputStream.
     *
     * @param state_id id of the partial state requested
     * @param ostream the OutputStream
     */
    void getState(K state_id, OutputStream ostream);

    /**
     * Allows an application to read a state through a provided InputStream.
     *
     * @param istream the InputStream
     */
    void setState(InputStream istream);

    /**
     * Allows an application to read a partial state through a provided InputStream.
     *
     * @param state_id id of the partial state requested
     * @param istream the InputStream
     */
    void setState(K state_id, InputStream istream);

    /**
     * Allows the replicator to set the local address from the channel
     * @param address
     */
    void setAddress(Serializable address);

    /**
     * Called when members are dropped
     * @param addresses
     */
    void droppedMembers(Collection<Serializable> addresses);

    /**
     * Return true if the object has the given state
     * @param state_id
     * @return
     */
    boolean hasState(K state_id);

}
