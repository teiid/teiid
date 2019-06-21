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

import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OptionalDataException;
import java.util.List;

import org.teiid.client.security.LogonResult;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.StringUtil;


/**
 * Represents the information needed in a socket connection handshake
 */
public class Handshake implements Externalizable {

    private static final long serialVersionUID = 7839271224736355515L;

    private String version = ApplicationInfo.getInstance().getReleaseNumber();
    private byte[] publicKey;
    private byte[] publicKeyLarge;
    private AuthenticationType authType = AuthenticationType.USERPASSWORD;
    private boolean cbc = true;

    public Handshake() {

    }

    Handshake(String version) {
        this.version = version;
    }

    /**
     * @return Returns the version.
     */
    public String getVersion() {
        if (this.version != null) {
            //normalize to allow for more increments
            StringBuilder builder = new StringBuilder();
            List<String> parts = StringUtil.split(this.version, "."); //$NON-NLS-1$
            for (int i = 0; i < parts.size(); i++) {
                if (i > 0) {
                    builder.append('.');
                }
                String part = parts.get(i);
                if (part.length() < 2 && Character.isDigit(part.charAt(0))) {
                    builder.append('0');
                }
                builder.append(part);
            }
            return builder.toString();
        }
        return this.version;
    }

    /**
     * Sets the version from the {@link ApplicationInfo}
     */
    public void setVersion() {
        this.version = ApplicationInfo.getInstance().getReleaseNumber();
    }

    /**
     * @return Returns the key.
     */
    public byte[] getPublicKey() {
        return this.publicKey;
    }

    /**
     * @param key The key to set.
     */
    public void setPublicKey(byte[] key) {
        this.publicKey = key;
    }

    /**
     * Represents the default auth type for the entire instance.
     * Per vdb auth types are now supported and provided in the {@link LogonResult}
     * @return
     */
    @Deprecated
    public AuthenticationType getAuthType() {
        return authType;
    }

    public void setAuthType(AuthenticationType authType) {
        this.authType = authType;
    }

    public byte[] getPublicKeyLarge() {
        return publicKeyLarge;
    }

    public void setPublicKeyLarge(byte[] publicKeyLarge) {
        this.publicKeyLarge = publicKeyLarge;
    }

    public boolean isCbc() {
        return cbc;
    }

    public void setCbc(boolean cbc) {
        this.cbc = cbc;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        version = (String)in.readObject();
        publicKey = (byte[])in.readObject();
        try {
            authType = AuthenticationType.values()[in.readByte()];
            int byteLength = in.readInt();
            if (byteLength > -1) {
                publicKeyLarge = new byte[byteLength];
                in.readFully(publicKeyLarge);
            }
        } catch (EOFException e) {
            publicKeyLarge = null;
        }
        try {
            cbc = in.readBoolean();
        } catch (OptionalDataException e) {
            cbc = false;
        } catch (EOFException e) {
            cbc = false;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(version);
        out.writeObject(publicKey);
        out.writeByte(authType.ordinal());
        if (publicKeyLarge == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(publicKeyLarge.length);
            out.write(publicKeyLarge);
        }
        out.writeBoolean(cbc);
    }

}
