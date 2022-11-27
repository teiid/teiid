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

package org.teiid.translator.jdbc.oracle;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Function;
import org.teiid.translator.jdbc.FunctionModifier;


/**
 * This a method for multiple use. It can be used for:
 * 1) dayofyear
 * 2) dayofmonth
 * 3) dayofweek
 * 4) week
 * 5) quarter
 */
public class DayWeekQuarterFunctionModifier extends FunctionModifier {
    private String format;

    public DayWeekQuarterFunctionModifier(String format) {
        this.format = format;
    }

    @Override
    public List<?> translate(Function function) {
        return Arrays.asList("to_number(TO_CHAR(",function.getParameters().get(0), ", '", format,"'))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}

