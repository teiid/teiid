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

/**
 * <p>Encodes and decodes to and from Base64 notation.
 */
public class Base64
{
    /**
     * Encodes a byte array into Base64 notation.
     *
     * @param source The data to convert
     * @since 1.4
     */
    public static String encodeBytes( byte[] source )
    {
        return java.util.Base64.getEncoder().encodeToString(source);
    }

    /**
     * Decodes data from Base64 notation.
     *
     * @param s the string to decode
     * @return the decoded data
     * @since 1.4
     */
    public static byte[] decode( CharSequence s )
    {
        return java.util.Base64.getDecoder().decode(s.toString());
    }

    public static String encodeUrlSafe(byte[] data) {
        return java.util.Base64.getUrlEncoder().encodeToString(data);
    }

    public static byte[] decodeUrlSafe(CharSequence data) {
        return java.util.Base64.getUrlDecoder().decode(data.toString());
    }

}
