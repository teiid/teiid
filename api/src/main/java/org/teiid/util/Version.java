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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.StringUtil;

/**
 * Represents a comparable version
 */
public class Version implements Comparable<Version> {

    public static Version DEFAULT_VERSION = new Version(new Integer[] {0});
    private static final Pattern NUMBER_PATTERN = Pattern.compile("(\\d+)"); //$NON-NLS-1$

    private Integer[] parts;

    public static Version getVersion(String version) {
        if (version == null) {
            return null;
        }
        String[] parts = version.split("\\."); //$NON-NLS-1$
        List<Integer> versionParts = new ArrayList<Integer>();
        for (String part : parts) {
            Integer val = null;
            Matcher m = NUMBER_PATTERN.matcher(part);
            if (!m.find()) {
                continue;
            }

            String num = m.group(1);
            try {
                val = Integer.parseInt(num);
            } catch (NumberFormatException e) {

            }
            versionParts.add(val == null ? 0 : val);
        }
        if (versionParts.isEmpty()) {
            return DEFAULT_VERSION;
        }
        return new Version(versionParts.toArray(new Integer[versionParts.size()]));
    }

    public Version(Integer[] parts) {
        this.parts = parts;
    }

    @Override
    public String toString() {
        return StringUtil.toString(this.parts, ".", false); //$NON-NLS-1$
    }

    public int getMajorVersion() {
        return parts[0];
    }

    @Override
    public int compareTo(Version o) {
        int length = Math.min(this.parts.length, o.parts.length);
        for (int i = 0; i < length; i++) {
            int comp = this.parts[i].compareTo(o.parts[i]);
            if (comp != 0) {
                return comp;
            }
        }
        if (this.parts.length > length) {
            return 1;
        }
        if (o.parts.length > length) {
            return -1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Version)) {
            return false;
        }
        return this.compareTo((Version)obj) == 0;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(parts);
    }

}
