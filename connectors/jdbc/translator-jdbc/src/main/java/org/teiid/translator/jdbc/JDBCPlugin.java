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

/*
 */
package org.teiid.translator.jdbc;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class JDBCPlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.jdbc" ; //$NON-NLS-1$

    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
                                                         PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$


    public static enum Event implements BundleUtil.Event{
        TEIID11002, // connection creation failed
        TEIID11003, // invalid hint
        TEIID11004,
        TEIID11005,
        TEIID11006,
        TEIID11008,
        TEIID11009,
        TEIID11010,
        TEIID11011,
        TEIID11012,
        TEIID11013,
        TEIID11014,
        TEIID11015,
        TEIID11016,
        TEIID11017,
        TEIID11018,
        TEIID11020,
        TEIID11021,
        TEIID11022,
        TEIID11023,
        TEIID11024,
        TEIID11025,
        TEIID11026,
        TEIID11027,
        TEIID11028,
        TEIID11029,
    }
}
