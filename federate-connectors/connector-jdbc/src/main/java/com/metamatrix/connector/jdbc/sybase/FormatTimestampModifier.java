/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc.sybase;

import java.util.*;
import java.util.HashMap;

import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicFunctionModifier;
import com.metamatrix.data.language.*;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;

/**
 */
public class FormatTimestampModifier extends BasicFunctionModifier implements FunctionModifier {

    private Map styleMappings;
    private ILanguageFactory langFactory;
    
    public FormatTimestampModifier(ILanguageFactory langFactory) {
        this.langFactory = langFactory;
        loadStyleMappings();
    }

    private void loadStyleMappings() {
        styleMappings = new HashMap();
        styleMappings.put("yyyy-MM-dd", new Integer(1));    // standard SQL format //$NON-NLS-1$
        styleMappings.put("MM/dd/yyyy", new Integer(1)); //$NON-NLS-1$
        styleMappings.put("yy/MM/dd", new Integer(2)); //$NON-NLS-1$
        styleMappings.put("dd/MM/yy", new Integer(3)); //$NON-NLS-1$
        styleMappings.put("dd.mm.yy", new Integer(4)); //$NON-NLS-1$
        styleMappings.put("dd-mm-yy", new Integer(5)); //$NON-NLS-1$
        styleMappings.put("dd mm yy", new Integer(6)); //$NON-NLS-1$
        styleMappings.put("MMM dd, yy", new Integer(7)); //$NON-NLS-1$
        styleMappings.put("HH:mm:ss", new Integer(8)); //$NON-NLS-1$
        styleMappings.put("MM dd yy hh:mm:ss:zzza", new Integer(9)); //$NON-NLS-1$
        styleMappings.put("MM-dd-yy", new Integer(10)); //$NON-NLS-1$
        styleMappings.put("yy/MM/dd", new Integer(11)); //$NON-NLS-1$
        styleMappings.put("yyMMdd", new Integer(12)); //$NON-NLS-1$
        styleMappings.put("yy/dd/MM", new Integer(13)); //$NON-NLS-1$
        styleMappings.put("MM/yy/dd", new Integer(14)); //$NON-NLS-1$
        styleMappings.put("dd/yy/MM", new Integer(15)); //$NON-NLS-1$
        styleMappings.put("MMM dd yy HH:mm:ss", new Integer(16)); //$NON-NLS-1$
        styleMappings.put("hh:mma", new Integer(17)); //$NON-NLS-1$
        styleMappings.put("HH:mm", new Integer(18)); //$NON-NLS-1$
        styleMappings.put("hh:mm:ss:zzza", new Integer(19)); //$NON-NLS-1$
        styleMappings.put("HH:mm:ss:zzz", new Integer(20)); //$NON-NLS-1$
    }

    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        IExpression[] args = function.getParameters();
        
        String format = (String) ((ILiteral)args[1]).getValue();

        Integer styleCode = (Integer) styleMappings.get(format);
        IFunction func = null;
        if(styleCode != null) { 
            func = langFactory.createFunction("convert", //$NON-NLS-1$
                new IExpression[] { 
                    langFactory.createLiteral("varchar", String.class),//$NON-NLS-1$
                    args[0],
                    langFactory.createLiteral(styleCode, Integer.class)                        
                },
                String.class);
                
        } else {
            func = langFactory.createFunction("convert", //$NON-NLS-1$
                new IExpression[] { 
                    langFactory.createLiteral("varchar", String.class),//$NON-NLS-1$
                    args[0]                        
                },
                String.class);
        }
        return func;
    }

}
