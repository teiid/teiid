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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.connector.DataPlugin;
import org.teiid.core.util.StringUtil;

/**
 * Defines a base schema that is the holder for namespace and type information
 */
public class NamespaceContainer implements Serializable {
    
    static final String TEIID_RESERVED = "teiid_"; //$NON-NLS-1$
    private static final String TEIID_SF = "teiid_sf"; //$NON-NLS-1$
    private static final String TEIID_RELATIONAL = "teiid_rel"; //$NON-NLS-1$
    private static final String TEIID_WS = "teiid_ws"; //$NON-NLS-1$
    private static final String TEIID_MONGO = "teiid_mongo"; //$NON-NLS-1$
    private static final String TEIID_ODATA = "teiid_odata"; //$NON-NLS-1$
    private static final String TEIID_ACCUMULO = "teiid_accumulo"; //$NON-NLS-1$
    private static final String TEIID_EXCEL = "teiid_excel"; //$NON-NLS-1$
    private static final String TEIID_JPA = "teiid_jpa"; //$NON-NLS-1$
    private static final String TEIID_HBASE = "teiid_hbase"; //$NON-NLS-1$
    private static final String TEIID_SPATIAL = "teiid_spatial"; //$NON-NLS-1$
    private static final String TEIID_LDAP = "teiid_ldap"; //$NON-NLS-1$
    private static final String TEIID_REST = "teiid_rest"; //$NON-NLS-1$
    private static final String TEIID_PI = "teiid_pi"; //$NON-NLS-1$
    private static final String TEIID_COUCHBASE = "teiid_couchbase"; //$NON-NLS-1$
    private static final String TEIID_INFINISPAN = "teiid_ispn"; //$NON-NLS-1$

    public static final String SF_URI = "{http://www.teiid.org/translator/salesforce/2012}"; //$NON-NLS-1$
    public static final String WS_URI = "{http://www.teiid.org/translator/ws/2012}"; //$NON-NLS-1$
    public static final String MONGO_URI = "{http://www.teiid.org/translator/mongodb/2013}"; //$NON-NLS-1$
    public static final String ODATA_URI = "{http://www.jboss.org/teiiddesigner/ext/odata/2012}"; //$NON-NLS-1$
    public static final String ACCUMULO_URI = "{http://www.teiid.org/translator/accumulo/2013}"; //$NON-NLS-1$
    public static final String EXCEL_URI = "{http://www.teiid.org/translator/excel/2014}"; //$NON-NLS-1$
    public static final String JPA_URI = "{http://www.teiid.org/translator/jpa/2014}"; //$NON-NLS-1$
    public static final String HBASE_URI = "{http://www.teiid.org/translator/hbase/2014}"; //$NON-NLS-1$
    public static final String SPATIAL_URI = "{http://www.teiid.org/translator/spatial/2015}"; //$NON-NLS-1$
    public static final String LDAP_URI = "{http://www.teiid.org/translator/ldap/2015}"; //$NON-NLS-1$
    public static final String REST_URI = "{http://teiid.org/rest}"; //$NON-NLS-1$
    public static final String PI_URI = "{http://www.teiid.org/translator/pi/2016}"; //$NON-NLS-1$
    public static final String COUCHBASE_URI = "{http://www.teiid.org/translator/couchbase/2017}"; //$NON-NLS-1$
    public static final String INFINISPAN_URI = "{http://www.teiid.org/translator/infinispan/2017}"; //$NON-NLS-1$

    public static final Map<String, String> BUILTIN_NAMESPACES;
    static {
        Map<String, String> map = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        map.put(TEIID_RELATIONAL, AbstractMetadataRecord.RELATIONAL_URI.substring(1, AbstractMetadataRecord.RELATIONAL_URI.length()-1));
        map.put(TEIID_SF, SF_URI.substring(1, SF_URI.length()-1));
        map.put(TEIID_WS, WS_URI.substring(1, WS_URI.length()-1));
        map.put(TEIID_MONGO, MONGO_URI.substring(1, MONGO_URI.length()-1));
        map.put(TEIID_ODATA, ODATA_URI.substring(1, ODATA_URI.length()-1));
        map.put(TEIID_ACCUMULO, ACCUMULO_URI.substring(1, ACCUMULO_URI.length()-1));
        map.put(TEIID_EXCEL, EXCEL_URI.substring(1, EXCEL_URI.length()-1));
        map.put(TEIID_JPA, JPA_URI.substring(1, JPA_URI.length()-1));
        map.put(TEIID_HBASE, HBASE_URI.substring(1, HBASE_URI.length()-1));
        map.put(TEIID_SPATIAL, SPATIAL_URI.substring(1, SPATIAL_URI.length()-1));
        map.put(TEIID_LDAP, LDAP_URI.substring(1, LDAP_URI.length()-1));
        map.put(TEIID_REST, REST_URI.substring(1, REST_URI.length()-1));
        map.put(TEIID_PI, PI_URI.substring(1, PI_URI.length()-1));
        map.put(TEIID_COUCHBASE, COUCHBASE_URI.substring(1, COUCHBASE_URI.length()-1));
        map.put(TEIID_INFINISPAN, INFINISPAN_URI.substring(1, INFINISPAN_URI.length()-1));
        BUILTIN_NAMESPACES = Collections.unmodifiableMap(map);
    }
    
    protected Map<String, String> namespaces;
    
    public void addNamespace(String prefix, String uri) {
        if (uri == null || uri.indexOf('}') != -1) {
            throw new MetadataException(DataPlugin.Event.TEIID60018, DataPlugin.Util.gs(DataPlugin.Event.TEIID60018, uri));
        }
        
        if (StringUtil.startsWithIgnoreCase(prefix, MetadataFactory.TEIID_RESERVED)) {
            String validURI = MetadataFactory.BUILTIN_NAMESPACES.get(prefix);
            if (validURI == null || !uri.equals(validURI)) {
                throw new MetadataException(DataPlugin.Event.TEIID60017, DataPlugin.Util.gs(DataPlugin.Event.TEIID60017, prefix));
            }
        }
        
        if (this.namespaces == null) {
             this.namespaces = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        }
        String old = this.namespaces.put(prefix, uri);
        if (old != null && !old.equals(uri)) {
            throw new MetadataException(DataPlugin.Event.TEIID60037, DataPlugin.Util.gs(DataPlugin.Event.TEIID60037, prefix, old, uri));
        }
    }
    
    public Map<String, String> getNamespaces() {
        if (this.namespaces == null) {
            return Collections.emptyMap();
        }
        return this.namespaces;
    }
    
    public static String resolvePropertyKey(NamespaceContainer baseSchema, String key) {
        int index = key.indexOf(':');
        if (index > 0 && index < key.length() - 1) {
            String prefix = key.substring(0, index);
            String uri = BUILTIN_NAMESPACES.get(prefix);
            if (uri == null && baseSchema != null) {
                uri = baseSchema.getNamespaces().get(prefix);
            }
            if (uri != null) {
                key = '{' +uri + '}' + key.substring(index + 1, key.length());
            }
            //TODO warnings or errors if not resolvable 
        }
        return key;
    }
    
}
