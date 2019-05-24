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
package org.teiid.translator.phoenix;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class PhoenixPlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.phoenix" ;  //$NON-NLS-1$

    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$

    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID, BUNDLE_NAME, ResourceBundle.getBundle(BUNDLE_NAME));

    public static enum Event implements BundleUtil.Event {

        // Phoenix HBase Table Mapping
        TEIID27001,

        // PhoenixQueryExecution
        TEIID27002,

        // PhoenixUpdateExecution
        TEIID27003,

        // HBaseProcedureExecution
        TEIID27004,

        // HBaseMetadataProcessor
        TEIID27005,

        TEIID27006,
        TEIID27007,
        TEIID27008,
        TEIID27009,
        TEIID27010,

        TEIID27011,
        TEIID27012,
        TEIID27013,
        TEIID27014,
        TEIID27015,
        TEIID27016,
        TEIID27017,
        TEIID27018,
        TEIID27019,
        TEIID27020,
    }
}