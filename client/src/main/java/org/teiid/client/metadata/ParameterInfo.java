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

package org.teiid.client.metadata;

import java.io.*;
import java.io.Externalizable;

/**
 * Desccribes some parameter info to return when executing a CallableStatement -
 * this is used to avoid sending SPParameter, which contains references to metadata
 * objects and stuff we don't want to send.
 */
public class ParameterInfo implements Externalizable {

    static final long serialVersionUID = -683851729051138932L;

    private int type;           // used outbound
    private int numColumns;     // if type is a result set - used outbound

    /** Constant identifying an IN parameter */
    public static final int IN = 1;

    /** Constant identifying an OUT parameter */
    public static final int OUT = 2;

    /** Constant identifying an INOUT parameter */
    public static final int INOUT = 3;

    /** Constant identifying a RETURN parameter */
    public static final int RETURN_VALUE = 4;

    /** Constant identifying a RESULT SET parameter */
    public static final int RESULT_SET = 5;

    // needed for Externalizable
    public ParameterInfo() {
    }

    /**
     * Create outbound parameter info
     * @param type
     * @param numColumns
     */
    public ParameterInfo(int type, int numColumns) {
        this.type = type;
        this.numColumns = numColumns;
    }

    public int getType() {
        return this.type;
    }

    public int getNumColumns() {
        return this.numColumns;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(type);
        out.writeInt(numColumns);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = in.readInt();
        numColumns = in.readInt();
    }



}
