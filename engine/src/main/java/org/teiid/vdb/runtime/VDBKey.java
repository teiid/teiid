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

package org.teiid.vdb.runtime;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.QueryPlugin;

/**
 * Encapsulates the name/versioning rules for VDBs
 */
public class VDBKey implements Serializable, Comparable<VDBKey>{

    private static final long serialVersionUID = -7249750823144856081L;

    private String name;
    private Integer[] versionParts = new Integer[3];
    private String version;
    private int hashCode;
    private boolean atMost;

    public static Pattern NAME_PATTERN = Pattern.compile("(?:(0|(?:[1-9]\\d{0,8})))(?:\\.(0|(?:[1-9]\\d{0,8})))?(?:\\.(0|(?:[1-9]\\d{0,8})))?(\\.)?$"); //$NON-NLS-1$

    public VDBKey(String name, Object version) {
        this.name = name;
        if (version == null) {
            //check for combination name
            int index = name.indexOf("."); //$NON-NLS-1$
            if (index > 0 && index < name.length() - 1) {
                this.name = name.substring(0, index);
                this.version = name.substring(index + 1);
            } else {
                //latest
                this.atMost = true;
            }
        } else {
            this.version = version.toString();

            //double check that the vdb is not ambiguous
            int index = name.indexOf("."); //$NON-NLS-1$
            if (index > 0 && index < name.length() - 1) {
                String possibleVersion = name.substring(index + 1);
                Matcher m  = NAME_PATTERN.matcher(possibleVersion);
                if (m.matches()) {
                    throw new TeiidRuntimeException(QueryPlugin.Event.TEIID31191, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31191, this.name));
                }
            }
        }

        if (this.version != null) {
            Matcher m  = NAME_PATTERN.matcher(this.version);
            if (!m.matches()) {
                if (version != null) {
                    throw new TeiidRuntimeException(QueryPlugin.Event.TEIID31190, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31190, this.version));
                }
                //not a version
                this.name = name;
                this.version = null;
                this.atMost = true;
            } else {
                getPart(m, 0);
                getPart(m, 1);
                getPart(m, 2);
                atMost = this.version.endsWith("."); //$NON-NLS-1$
            }
        }

        //set the version string
        //TODO we may want to canonicalize to drop all .0
        if (this.versionParts[1] != null) {
            this.version = getSemanticVersion();
        } else if (this.versionParts[0] != null){
            this.version = this.versionParts[0].toString();
        }
    }

    private void getPart(Matcher m, int part) {
        String val = m.group(part+1);
        if (val != null) {
            versionParts[part] = Integer.parseInt(val);
        }
    }

    public String getName() {
        return name;
    }

    /**
     * Get the version string - not including the at most designation.
     * <br>
     * Will be the full semantic version if more than 1 part is specified.
     * <br>
     * Will be the first integer part if only it is specified.
     * <br>
     * Will be null if no version is present.
     * @return
     */
    public String getVersion() {
        return version;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = HashCodeUtil.hashCode(HashCodeUtil.expHashCode(name.toLowerCase(), false), getSemanticVersion());
        }
        return hashCode;
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof VDBKey)) {
            return false;
        }

        VDBKey other = (VDBKey)obj;

        return other.atMost == this.atMost
                && this.compareTo(other) == 0;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        sb.append("."); //$NON-NLS-1$
        if (version != null) {
            sb.append(version);
            if (atMost) {
                sb.append("."); //$NON-NLS-1$
            }
        } else {
            sb.append("latest"); //$NON-NLS-1$
        }
        return sb.toString();
    }

    @Override
    public int compareTo(VDBKey o) {
        int compare = String.CASE_INSENSITIVE_ORDER.compare(name, o.name);
        if (compare != 0) {
            return compare;
        }
        for (int i = 0; i < versionParts.length; i++) {
            compare = Integer.compare(versionParts[i]==null?0:versionParts[i], o.versionParts[i]==null?0:o.versionParts[i]);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    /**
     * @return true if the semantic version ends in a .
     */
    public boolean isAtMost() {
        return atMost;
    }

    /**
     * @param key
     * @return true if the key version &gt;= the current version
     */
    public boolean acceptsVerion(VDBKey key) {
        if (!key.getName().equalsIgnoreCase(getName())) {
            return false;
        }
        for (int i = 0; i < versionParts.length; i++) {
            if (versionParts[i] == null) {
                break;
            }
            if (versionParts[i].intValue() != (key.versionParts[i]==null?0:key.versionParts[i].intValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the full three part semantic version
     * @return
     */
    public String getSemanticVersion() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < versionParts.length; i++) {
            if (i > 0) {
                sb.append("."); //$NON-NLS-1$
            }
            if (versionParts[i] == null) {
                sb.append(0);
            } else {
                sb.append(versionParts[i]);
            }
        }
        return sb.toString();
    }

}
