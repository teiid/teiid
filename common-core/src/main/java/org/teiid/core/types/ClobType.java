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

package org.teiid.core.types;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OptionalDataException;
import java.sql.Clob;

import org.teiid.core.util.ExternalizeUtil;


/**
 * This is wrapper on top of a "clob" object, which implements the "java.sql.Clob"
 * interface. This class also implements the Streamable interface
 */
public final class ClobType extends BaseClobType {

    public enum Type {
        TEXT, JSON
    }

    private static final long serialVersionUID = 2753412502127824104L;

    private Type type = Type.TEXT;

    public ClobType() {
    }

    public ClobType(Clob clob) {
        super(clob);
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        try {
            this.type = ExternalizeUtil.readEnum(in, Type.class, Type.TEXT);
        } catch (OptionalDataException e) {
            this.type = Type.TEXT;
        } catch(IOException e) {
            this.type = Type.TEXT;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        ExternalizeUtil.writeEnum(out, this.type);
    }

}
