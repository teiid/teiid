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

package org.teiid.core.util;

import java.io.ByteArrayOutputStream;

public class AccessibleByteArrayOutputStream extends ByteArrayOutputStream {

    public AccessibleByteArrayOutputStream() {
        super();
    }

    public AccessibleByteArrayOutputStream(int size) {
        super(size);
    }

    public byte[] getBuffer() {
        return this.buf;
    }

    public void setBuffer(byte[] buffer) {
        this.buf = buffer;
    }

    public int getCount() {
        return this.count;
    }

    public void setCount(int count) {
        this.count = count;
    }

}