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

package org.teiid.connector.jdbc.translator;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.connector.language.IFunction;

/**
 * Wrap a function in standard JDBC escape syntax.  In some cases, the 
 * driver can then convert to the correct database syntax for us. 
 * @since 5.0
 */
public class EscapeSyntaxModifier extends BasicFunctionModifier {

    public EscapeSyntaxModifier() {
        super();
    }
    
    /** 
     * @see org.teiid.connector.jdbc.translator.BasicFunctionModifier#translate(com.metamatrix.connector.language.IFunction)
     * @since 5.0
     */
    public List translate(IFunction function) {
        List normalParts = super.translate(function);
        List wrappedParts = new ArrayList(normalParts.size() + 2);
        wrappedParts.add("{fn "); //$NON-NLS-1$
        wrappedParts.addAll(normalParts);
        wrappedParts.add("}"); //$NON-NLS-1$
        return wrappedParts;
    }

}
