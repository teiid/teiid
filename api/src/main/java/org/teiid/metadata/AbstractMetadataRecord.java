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

package org.teiid.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.StringUtil;


/**
 * AbstractMetadataRecord
 */
public abstract class AbstractMetadataRecord implements Serializable {

    private static final Collection<AbstractMetadataRecord> EMPTY_INCOMING = Collections.emptyList();

    public interface Modifiable {
        long getLastModified();
    }

    public interface DataModifiable {
        public static final String DATA_TTL = AbstractMetadataRecord.RELATIONAL_PREFIX + "data-ttl"; //$NON-NLS-1$

        long getLastDataModification();
    }

    private static final long serialVersionUID = 564092984812414058L;

    public final static char NAME_DELIM_CHAR = '.';

    private static AtomicLong UUID_SEQUENCE = new AtomicLong();

    private String uuid; //globally unique id
    private String name; //contextually unique name

    private String nameInSource;

    private volatile Map<String, String> properties;
    private String annotation;

    private transient Collection<AbstractMetadataRecord> incomingObjects;

    public static final String RELATIONAL_PREFIX = NamespaceContainer.RELATIONAL_PREFIX;

    public String getUUID() {
        if (uuid == null) {
            uuid = String.valueOf(UUID_SEQUENCE.getAndIncrement());
        }
        return uuid;
    }

    public void setUUID(String uuid) {
        this.uuid = uuid;
    }

    public String getNameInSource() {
        return nameInSource;
    }

    public void setNameInSource(String nameInSource) {
        this.nameInSource = DataTypeManager.getCanonicalString(nameInSource);
    }

    /**
     * Get the name in source or the name if
     * the name in source is not set.
     * @return
     */
    public String getSourceName() {
        if (this.nameInSource != null && this.nameInSource.length() > 0) {
            return this.nameInSource;
        }
        return getName();
    }

    /**
     * WARNING - The name returned by this method may be ambiguous and
     * is not SQL safe - it may need quoted/escaped
     */
    public String getFullName() {
        AbstractMetadataRecord parent = getParent();
        if (parent != null) {
            String result = parent.getFullName() + NAME_DELIM_CHAR + getName();
            return result;
        }
        return name;
    }

    public void getSQLString(StringBuilder sb) {
        AbstractMetadataRecord parent = getParent();
        if (parent != null) {
            parent.getSQLString(sb);
            sb.append(NAME_DELIM_CHAR);
        }
        sb.append('"').append(StringUtil.replace(name, "\"", "\"\"")).append('"'); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Get the full name as a SQL safe string
     * @return
     */
    public String getSQLString() {
        StringBuilder sb = new StringBuilder();
        getSQLString(sb);
        return sb.toString();
    }

    public AbstractMetadataRecord getParent() {
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = DataTypeManager.getCanonicalString(name);
    }

    public String getCanonicalName() {
        return name.toUpperCase();
    }

    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(" name="); //$NON-NLS-1$
        sb.append(getName());
        sb.append(", nameInSource="); //$NON-NLS-1$
        sb.append(getNameInSource());
        sb.append(", uuid="); //$NON-NLS-1$
        sb.append(getUUID());
        return sb.toString();
    }

    /**
     * Return the extension properties for this record - may be unmodifiable
     * if {@link #setProperties(Map)} or {@link #setProperty(String, String)}
     * has not been called.
     * @return
     */
    public Map<String, String> getProperties() {
        if (properties == null) {
            return Collections.emptyMap();
        }
        return properties;
    }

    /**
     * Get the property value for the given key.
     * <br>
     * If the key is in the form of a legacy uri fqn, the prefix version will also be checked.
     * This behavior will be removed in a later release.
     * @param key
     * @return
     */
    public String getProperty(String key) {
        return getProperty(key, false);
    }

    /**
     * @see AbstractMetadataRecord#getProperty(String)
     * @param key
     * @param checkUnqualified
     * @return
     */
    @Deprecated()
    public String getProperty(String key, boolean checkUnqualified) {
        String value = getProperties().get(key);
        if (value != null) {
            return value;
        }
        if (!checkUnqualified) {
            String newKey = NamespaceContainer.resolvePropertyKey(key);
            if (newKey.equals(key)) {
                return null;
            }
            value = getProperties().get(newKey);
            if (value != null) {
                return value;
            }
        }
        int index = key.indexOf('}');
        if (index > 0 && index < key.length() &&  key.charAt(0) == '{') {
            key = key.substring(index + 1, key.length());
        } else {
            index = key.indexOf(':');
            if (index > 0 && index < key.length()) {
                key = key.substring(index + 1, key.length());
            }
        }
        return getProperties().get(key);
    }

    /**
     * The preferred setter for extension properties.
     * @param key
     * @param value if null the property will be removed
     */
    public String setProperty(String key, String value) {
        if (this.properties == null) {
            synchronized (this) {
                if (this.properties == null && value == null) {
                    return null;
                }
                this.properties = new ConcurrentSkipListMap<String, String>(String.CASE_INSENSITIVE_ORDER);
            }
        }
        if (value == null) {
            return this.properties.remove(key);
        }
        return this.properties.put(DataTypeManager.getCanonicalString(key), DataTypeManager.getCanonicalString(value));
    }

    public synchronized void setProperties(Map<String, String> properties) {
        if (this.properties == null) {
            this.properties = new ConcurrentSkipListMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        } else {
            this.properties.clear();
        }
        if (properties != null) {
            this.properties.putAll(properties);
        }
    }

    public String getAnnotation() {
        return annotation;
    }

    public void setAnnotation(String annotation) {
        this.annotation = DataTypeManager.getCanonicalString(annotation);
    }

    /**
     * Compare two records for equality.
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(obj.getClass() != this.getClass()) {
            return false;
        }

        AbstractMetadataRecord other = (AbstractMetadataRecord)obj;

        return EquivalenceUtil.areEqual(this.getUUID(), other.getUUID());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (this.properties != null && !(this.properties instanceof ConcurrentSkipListMap<?, ?>)) {
            this.properties = new ConcurrentSkipListMap<String, String>(this.properties);
        }
    }

    public int hashCode() {
        return getUUID().hashCode();
    }

    /**
     * Objects used to make this object.  Never null.
     * @return
     */
    public Collection<AbstractMetadataRecord> getIncomingObjects() {
        if (incomingObjects == null) {
            return EMPTY_INCOMING;
        }
        return incomingObjects;
    }

    public void setIncomingObjects(Collection<AbstractMetadataRecord> incomingObjects) {
        this.incomingObjects = incomingObjects;
    }

    public boolean isUUIDSet() {
        return this.uuid != null && this.uuid.length() > 0 && !Character.isDigit(this.uuid.charAt(0));
    }

    /**
     * Get a context unique identifier for this object.  Typically it's just the name.
     * @return
     */
    public String getIdentifier() {
        return this.getName();
    }

}