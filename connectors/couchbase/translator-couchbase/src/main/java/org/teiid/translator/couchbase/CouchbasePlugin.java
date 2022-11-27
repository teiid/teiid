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
package org.teiid.translator.couchbase;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class CouchbasePlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.couchbase" ; //$NON-NLS-1$

    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$

    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,BUNDLE_NAME,ResourceBundle.getBundle(BUNDLE_NAME));

    public static enum Event implements BundleUtil.Event{
        TEIID29001,
        TEIID29002,
        TEIID29003,
        TEIID29004,
        TEIID29005,
        TEIID29006,
        TEIID29008,
        TEIID29009,
        TEIID29010,
        TEIID29011,
        TEIID29012,
        TEIID29013,
        TEIID29014,
        TEIID29015,
        TEIID29016,
        TEIID29017,
        TEIID29018,
        TEIID29019,
        TEIID29020,
        TEIID29021,
        TEIID29022,
        TEIID29023,
        TEIID29024,
        TEIID29025
    }
}
