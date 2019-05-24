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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.teiid.client.util.ExceptionHolder;

/**
 * A simple message holder.  To indicate an exception result,
 * the key is set to an {@link ExceptionHolder}
 */
public class Message implements Externalizable {
    public static final long serialVersionUID = 1063704220782714098L;
    private Object contents;
    private Serializable messageKey;

    public String toString() {
        return "MessageHolder: key=" + messageKey + " contents=" + contents; //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void setContents(Object contents) {
        this.contents = contents;
    }

    public Object getContents() {
        return contents;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        this.contents = in.readObject();
        this.messageKey = (Serializable) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.contents);
        out.writeObject(messageKey);
    }

    public Serializable getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(Serializable messageKey) {
        this.messageKey = messageKey;
    }

}
