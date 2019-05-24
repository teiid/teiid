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

package org.teiid.net.socket;

import org.teiid.core.BundleUtil;
import org.teiid.net.CommunicationException;


/**
 * An error occurred in communication between client and server.  This
 * error may or may not be recoverable.  Generally the communication
 * transport should be able to tell the difference and recover if possible.
 */
public class SingleInstanceCommunicationException extends CommunicationException {
    /**
     * No-Arg Constructor
     */
    public SingleInstanceCommunicationException(  ) {
        super( );
    }

    /**
     * @param message
     */
    public SingleInstanceCommunicationException(String message) {
        super(message);
    }


    /**
     * @param e
     */
    public SingleInstanceCommunicationException(Throwable e) {
        super(e);
    }

    /**
     * @param e
     * @param message
     */
    public SingleInstanceCommunicationException(Throwable e, String message) {
        super(e, message);
    }

    public SingleInstanceCommunicationException(BundleUtil.Event event, Throwable e, String message) {
        super(event, e, message);
    }
}
