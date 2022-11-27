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

package org.teiid.translator.odata;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class ODataPlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.odata" ; //$NON-NLS-1$

    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID, PLUGIN_ID
            + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$

    public static enum Event implements BundleUtil.Event{
        TEIID17001,
        TEIID17002,
        TEIID17003,
        TEIID17004,
        TEIID17005,
        TEIID17006,
        TEIID17007,
        TEIID17008,
        TEIID17009,
        TEIID17010,    // ex
        TEIID17011,
        TEIID17012,
        TEIID17013,
        TEIID17014,
        TEIID17015,
        TEIID17016,
        TEIID17017,
        TEIID17018
    }
}
