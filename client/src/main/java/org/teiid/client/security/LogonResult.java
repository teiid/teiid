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

import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OptionalDataException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.teiid.core.util.ExternalizeUtil;



/**
 * Dataholder for the result of <code>ILogon.logon()</code>.
 * Contains a sessionID
 *
 * Analogous to the server side SessionToken
 */
public class LogonResult implements Externalizable {

    private static final long serialVersionUID = 4481443514871448269L;
    private TimeZone timeZone = TimeZone.getDefault();
    private String clusterName;
    private SessionToken sessionToken;
    private String vdbName;
    private int vdbVersion;
    private Map<Object, Object> addtionalProperties;

    public LogonResult() {
    }

    public LogonResult(SessionToken token, String vdbName, String clusterName) {
        this.clusterName = clusterName;
        this.sessionToken = token;
        this.vdbName = vdbName;
    }

    /**
     * Get the sessionID.
     * @return
     * @since 4.3
     */
    public String getSessionID() {
        return this.sessionToken.getSessionID();
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }


    public String getUserName() {
        return this.sessionToken.getUsername();
    }

    public String getClusterName() {
        return clusterName;
    }

    public SessionToken getSessionToken() {
        return sessionToken;
    }

    public String getVdbName() {
        return vdbName;
    }

    public int getVdbVersion() {
        return vdbVersion;
    }

    public Object getProperty(String key) {
        if (this.addtionalProperties == null) {
            return null;
        }
        return addtionalProperties.get(key);
    }

    public void addProperty(String key, Object value) {
        if (this.addtionalProperties == null) {
            this.addtionalProperties = new HashMap<Object, Object>();
        }
        this.addtionalProperties.put(key, value);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        vdbName = (String)in.readObject();
        sessionToken = (SessionToken)in.readObject();
        try {
            timeZone = (TimeZone) in.readObject();
        } catch (Exception e) {
            //could be a sun.util object
        }
        clusterName = (String)in.readObject();
        vdbVersion = in.readInt();
        try {
            addtionalProperties = ExternalizeUtil.readMap(in);
            String tzId = in.readUTF(); //not sent until 8.12.3
            timeZone = TimeZone.getTimeZone(tzId);
        } catch (EOFException e) {

        } catch (OptionalDataException e) {

        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(vdbName);
        out.writeObject(sessionToken);
        out.writeObject(timeZone);
        out.writeObject(clusterName);
        out.writeInt(vdbVersion);
        ExternalizeUtil.writeMap(out, addtionalProperties);
        out.writeUTF(timeZone.getID());
    }

}
