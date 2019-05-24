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

import java.util.Arrays;

import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;

public final class BinaryType implements Comparable<BinaryType> {

    private byte[] bytes;

    public BinaryType(byte[] bytes) {
        Assertion.isNotNull(bytes);
        //to be truly immutable we should clone here
        this.bytes = bytes;
    }

    /**
     *
     * @return the actual bytes - no modifications should be performed
     */
    public byte[] getBytesDirect() {
        return this.bytes;
    }

    /**
     *
     * @return a copy of the bytes
     */
    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * Get the byte value at a given index
     * @param index
     */
    public byte getByte(int index) {
        return bytes[index];
    }

    public int getLength() {
        return bytes.length;
    }

    @Override
    public int compareTo(BinaryType o) {
        int len1 = getLength();
        int len2 = o.getLength();
        int n = Math.min(len1, len2);
        for (int i = 0; i < n; i++) {
            //unsigned comparison
            int b1 = bytes[i] & 0xff;
            int b2 = o.bytes[i] & 0xff;
            if (b1 != b2) {
                return b1 - b2;
            }
        }
        return len1 - len2;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BinaryType)) {
            return false;
        }
        BinaryType other = (BinaryType)obj;
        return Arrays.equals(this.bytes, other.bytes);
    }

    /**
     * Returns the hex string representing the binary value.
     */
    @Override
    public String toString() {
        return PropertiesUtils.toHex(bytes);
    }

    public BlobType toBlob() {
        return new BlobType(BlobType.createBlob(bytes));
    }

}
