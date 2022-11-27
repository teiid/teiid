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

package org.teiid.core;

import java.util.ResourceBundle;


/**
 * CorePlugin
 */
public class CorePlugin {
    //
    // Class Constants:
    //
    /**
     * The plug-in identifier of this plugin
     */
    public static final String PLUGIN_ID = CorePlugin.class.getPackage().getName();

    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
                                                         PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$

    public static enum Event implements BundleUtil.Event {
        TEIID10000,
        TEIID10001,
        TEIID10002,
        TEIID10003,
        TEIID10004,
        TEIID10005,
        TEIID10006,
        TEIID10009,
        TEIID10010,
        TEIID10011,
        TEIID10012,
        TEIID10013,
        TEIID10016,
        TEIID10017,
        TEIID10018,
        TEIID10021,
        TEIID10022,
        TEIID10023,
        TEIID10024,
        TEIID10030,
        TEIID10032,
        TEIID10033,
        TEIID10034,
        TEIID10035,
        TEIID10036,
        TEIID10037,
        TEIID10038,
        TEIID10039,
        TEIID10040,
        TEIID10041,
        TEIID10042,
        TEIID10043,
        TEIID10044,
        TEIID10045,
        TEIID10046,
        TEIID10047,
        TEIID10048,
        TEIID10049,
        TEIID10051,
        TEIID10052,
        TEIID10053,
        TEIID10054,
        TEIID10056,
        TEIID10057,
        TEIID10058,
        TEIID10059,
        TEIID10060,
        TEIID10061,
        TEIID10063,
        TEIID10068,
        TEIID10070,
        TEIID10071,
        TEIID10072,
        TEIID10073,
        TEIID10074,
        TEIID10076,
        TEIID10077,
        TEIID10078,
        TEIID10080,
        TEIID10081,
        TEIID10082,
        TEIID10083,
        TEIID10084,
    }
}
