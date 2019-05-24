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

package org.teiid.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

public class WSUtil {

    public static String appendQueryString(String endpoint, String param) {
        return endpoint + (endpoint.indexOf('?') >= 0?'&':'?') + param;
    }

    public static String httpURLEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeSource(final Source source) {
        if (!(source instanceof StreamSource)) {
            return;
        }

        StreamSource stream = (StreamSource)source;
        try {
            if (stream.getInputStream() != null) {
                stream.getInputStream().close();
            }
        } catch (IOException e) {
        }
        try {
            if (stream.getReader() != null) {
                stream.getReader().close();
            }
        } catch (IOException e) {
        }
    }

}