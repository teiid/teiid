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

package org.teiid.test.util;


/**
 * This is a common place to put String utility methods.
 */
public final class StringUtil {


    
    public static String removeChars(final String value, final char[] chars) {
        final StringBuffer result = new StringBuffer();
        if (value != null && chars != null && chars.length > 0) {
            final String removeChars = String.valueOf(chars);
            for (int i = 0; i < value.length(); i++) {
                final String character = value.substring(i, i + 1);
                if (removeChars.indexOf(character) == -1) {
                    result.append(character);
                }
            }
        } else {
            result.append(value);
        }
        return result.toString();
    }


}
