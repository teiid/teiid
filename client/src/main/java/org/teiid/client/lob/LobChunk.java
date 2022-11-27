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

package org.teiid.client.lob;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * A Lob Chunk object which carries information packets in byte streams. This
 * class used as value object to transfer blob object's data chunk back and forth
 * between the client and server.
 */
public class LobChunk implements Externalizable {
    static final long serialVersionUID = -5634014429424520672L;

    private byte[] data;
    private boolean last = false;

    public LobChunk() {

    }

    public LobChunk(byte[] data, boolean last){
        this.last = last;
        this.data = data;
    }

    public byte[] getBytes() {
        return this.data;
    }

    public boolean isLast() {
        return this.last;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        data = (byte[])in.readObject();
        last = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
        out.writeBoolean(last);
    }

}
