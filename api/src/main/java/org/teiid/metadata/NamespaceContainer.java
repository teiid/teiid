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
import java.util.HashMap;
import java.util.Map;

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
    private static final String TEIID_PARQUET = "teiid_parquet"; //$NON-NLS-1$
    private static final String TEIID_JPA = "teiid_jpa"; //$NON-NLS-1$
    private static final String TEIID_HBASE = "teiid_hbase"; //$NON-NLS-1$
    private static final String TEIID_SPATIAL = "teiid_spatial"; //$NON-NLS-1$
    private static final String TEIID_LDAP = "teiid_ldap"; //$NON-NLS-1$
    private static final String TEIID_REST = "teiid_rest"; //$NON-NLS-1$
    private static final String TEIID_PI = "teiid_pi"; //$NON-NLS-1$
    private static final String TEIID_COUCHBASE = "teiid_couchbase"; //$NON-NLS-1$
    private static final String TEIID_INFINISPAN = "teiid_ispn"; //$NON-NLS-1$

    public static final String SF_PREFIX = TEIID_SF+":"; //$NON-NLS-1$
    public static final String WS_PREFIX = TEIID_WS+":"; //$NON-NLS-1$
    public static final String MONGO_PREFIX = TEIID_MONGO+":"; //$NON-NLS-1$
    public static final String ODATA_PREFIX = TEIID_ODATA+":"; //$NON-NLS-1$
    public static final String ACCUMULO_PREFIX = TEIID_ACCUMULO+":"; //$NON-NLS-1$
    public static final String EXCEL_PREFIX = TEIID_EXCEL+":"; //$NON-NLS-1$
    public static final String PARQUET_PREFIX = TEIID_PARQUET+":"; //$NON-NLS-1$
    public static final String JPA_PREFIX = TEIID_JPA+":"; //$NON-NLS-1$
    public static final String HBASE_PREFIX = TEIID_HBASE+":"; //$NON-NLS-1$
    public static final String SPATIAL_PREFIX = TEIID_SPATIAL+":"; //$NON-NLS-1$
    public static final String LDAP_PREFIX = TEIID_LDAP+":"; //$NON-NLS-1$
    public static final String REST_PREFIX = TEIID_REST+":"; //$NON-NLS-1$
    public static final String PI_PREFIX = TEIID_PI+":"; //$NON-NLS-1$
    public static final String COUCHBASE_PREFIX = TEIID_COUCHBASE+":"; //$NON-NLS-1$
    public static final String INFINISPAN_PREFIX = TEIID_INFINISPAN+":"; //$NON-NLS-1$
    public static final String RELATIONAL_PREFIX = NamespaceContainer.TEIID_RELATIONAL+":"; //$NON-NLS-1$

    private static final Map<String, String> BUILTIN = new HashMap<String, String>() {

        public String put(String key, String value) {
            super.put(value.substring(0, value.length() - 1), key);
            return super.put(key, value);
        };

    };
    static {
        BUILTIN.put("{http://www.teiid.org/ext/relational/2012}", RELATIONAL_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/salesforce/2012}", SF_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/ws/2012}", WS_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/mongodb/2013}", MONGO_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.jboss.org/teiiddesigner/ext/odata/2012}", ODATA_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/accumulo/2013}", ACCUMULO_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/excel/2014}", EXCEL_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/jpa/2014}", JPA_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/hbase/2014}", HBASE_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/spatial/2015}", SPATIAL_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/ldap/2015}", LDAP_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://teiid.org/rest}", REST_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/pi/2016}", PI_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/couchbase/2017}", COUCHBASE_PREFIX); //$NON-NLS-1$
        BUILTIN.put("{http://www.teiid.org/translator/infinispan/2017}", INFINISPAN_PREFIX); //$NON-NLS-1$
    }

    public void addNamespace(String prefix, String uri) {
        if (uri == null || uri.indexOf('}') != -1) {
            throw new MetadataException(DataPlugin.Event.TEIID60018, DataPlugin.Util.gs(DataPlugin.Event.TEIID60018, uri));
        }

        if (StringUtil.startsWithIgnoreCase(prefix, MetadataFactory.TEIID_RESERVED)) {
            String validURI = BUILTIN.get(prefix);
            if (validURI == null || !uri.equals(validURI.substring(1, validURI.length()-1))) {
                throw new MetadataException(DataPlugin.Event.TEIID60017, DataPlugin.Util.gs(DataPlugin.Event.TEIID60017, prefix));
            }
            return;
        }

        throw new MetadataException(DataPlugin.Event.TEIID60037, DataPlugin.Util.gs(DataPlugin.Event.TEIID60037, prefix, uri));
    }


    public static String resolvePropertyKey(String key) {
        int index = key.indexOf('}');
        if (index > 0 && index < key.length() &&  key.charAt(0) == '{') {
            String uri_prefix = key.substring(0, index + 1);
            String prefix = BUILTIN.get(uri_prefix);
            if (prefix != null) {
                key = prefix + key.substring(index+1, key.length());
            }
        }

        return key;
    }

    public static String getLegacyKey(String key) {
        int index = key.indexOf(':');
        if (index > 0 && index < key.length()) {
            String uri_prefix = BUILTIN.get(key.substring(0, index));
            if (uri_prefix != null) {
                return uri_prefix + key.substring(index + 1, key.length());
            }
        }
        return null;
    }

}
