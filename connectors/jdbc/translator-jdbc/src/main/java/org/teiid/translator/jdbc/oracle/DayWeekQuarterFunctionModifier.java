/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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

