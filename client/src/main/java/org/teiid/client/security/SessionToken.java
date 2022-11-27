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

package org.teiid.client.security;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.SecureRandom;
import java.util.Arrays;

import org.teiid.core.util.Base64;


/**
 * This class is an immutable identifier for a unique session that also
 * maintains the name of the principal for that session.
 *
 * Since this class can be used to authenticate a user, it must be secure in
 * transit if sent to the client.  Also it should only be sent to the client
 * who creates the session.
 */
public class SessionToken implements Externalizable {
    public final static long serialVersionUID = -2853708320435636107L;

    private static final SecureRandom random = new SecureRandom();

    /** The session ID */
    private String sessionID;
    private String userName;
    private byte[] secret = new byte[16];

    public SessionToken() {
    }

    /**
     * Used by tests to control the session id
     *
     * @param id
     * @param userName
     */
    public SessionToken(long id, String userName) {
        this.sessionID = Long.toString(id);
        this.userName = userName;
    }

    /**
     * The primary constructor that specifies userName
     *
     * @param userName
     *         (String) the userName for this session
     */
    public SessionToken(String userName) {
        byte[] bytes = new byte[9]; //9 bytes fits evenly into base64 and should be sufficiently cluster unique
        random.nextBytes(bytes);
        this.sessionID = Base64.encodeBytes(bytes);
        this.userName = userName;
        random.nextBytes(secret);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SessionToken)) {
            return false;
        }
        SessionToken other = (SessionToken)obj;
        return userName.equals(other.userName)
            && sessionID.equals(other.sessionID)
            && Arrays.equals(secret, other.secret);
    }

    /**
     * Returns unique session identifier
     *
     * @return the session ID
     */
    public String getSessionID() {
        return this.sessionID;
    }

    /**
     * Get the principal name for this session's user.
     *
     * @return the user name
     */
    public String getUsername() {
        return this.userName;
    }

    /**
     * Returns a string representing the current state of the object.
     */
    public String toString() {
        return "SessionToken[" + getUsername() + "," + this.sessionID + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        secret = (byte[])in.readObject();
        sessionID = (String)in.readObject();
        userName = (String)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(secret);
        out.writeObject(sessionID);
        out.writeObject(userName);
    }

}
