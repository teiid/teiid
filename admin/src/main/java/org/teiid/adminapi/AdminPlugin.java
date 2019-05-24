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

package org.teiid.adminapi;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class AdminPlugin {
    public static final String PLUGIN_ID = "org.teiid.adminapi" ; //$NON-NLS-1$
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID, PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$

    public static enum Event implements BundleUtil.Event {
        TEIID70000,
        TEIID70003,
        TEIID70004,
        TEIID70005,
        TEIID70006,
        TEIID70007,
        TEIID70008,
        TEIID70009,
        TEIID70010,
        TEIID70011,
        TEIID70013,
        TEIID70014,
        TEIID70015,
        TEIID70016,
        TEIID70020,
        TEIID70021,
        TEIID70022,
        TEIID70023,
        TEIID70024,
        TEIID70025,
        TEIID70026,
        TEIID70027,
        TEIID70028,
        TEIID70029,
        TEIID70030,
        TEIID70031,
        TEIID70032,
        TEIID70033,
        TEIID70034,
        TEIID70035,
        TEIID70036,
        TEIID70037,
        TEIID70038,
        TEIID70039,
        TEIID70040,
        TEIID70041,
        TEIID70042,
        TEIID70043,
        TEIID70044,
        TEIID70045,
        TEIID70046,
        TEIID70047,
        TEIID70048,
        TEIID70049,
        TEIID70050,
        TEIID70051,
        TEIID70052,
        TEIID70053,
        TEIID70054,
        TEIID70055,
        TEIID70056,
        TEIID70057,
        TEIID70058
    }
}
