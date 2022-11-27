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

package org.teiid.query.sql.lang;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.WindowFrame.BoundMode;
import org.teiid.language.WindowFrame.FrameMode;
import org.teiid.query.sql.symbol.WindowFrame;
import org.teiid.query.sql.symbol.WindowFrame.FrameBound;


public class TestWindowFrame {

    @Test public void testEquals() {
        WindowFrame frame = new WindowFrame(FrameMode.RANGE);
        frame.setStart(new FrameBound(BoundMode.PRECEDING).bound(1));
        frame.setEnd(new FrameBound(BoundMode.CURRENT_ROW));

        UnitTestUtil.helpTestEquivalence(0, frame, frame);

        WindowFrame clone = frame.clone();
        UnitTestUtil.helpTestEquivalence(0, frame, clone);

        clone.setEnd(null);
        UnitTestUtil.helpTestEquivalence(1, frame, clone);
    }

}
