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

package com.metamatrix.connector.jdbc.db2;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.language.IFromItem;
import com.metamatrix.connector.language.IJoin;
import com.metamatrix.connector.language.ILimit;

/** 
 * @since 4.3
 */
public class DB2SQLConversionVisitor extends SQLConversionVisitor {

    private final int MAX_SELECT_ALIAS_LENGTH = 30;
    
    private final int MAX_TABLE_ALIAS_LENGTH = 128;
    
    /**
     * Convert limit clause to DB2 ...FETCH FIRST rowlimit ROWS ONLY syntax
     * @see com.metamatrix.connector.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IQuery)
     * @since 5.0 SP1
     */
    public void visit(ILimit obj) {
        buffer.append("FETCH") //$NON-NLS-1$
              .append(SPACE)
              .append("FIRST") //$NON-NLS-1$
              .append(SPACE)
              .append(obj.getRowLimit())
              .append(SPACE)
              .append("ROWS") //$NON-NLS-1$
              .append(SPACE)
              .append("ONLY"); //$NON-NLS-1$
    }
    
    /** 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#getMaxSelectAliasLength()
     * @since 4.3
     */
    protected int getMaxSelectAliasLength() {
        return MAX_SELECT_ALIAS_LENGTH;
    }

    /** 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#getMaxTableAliasLength()
     * @since 4.3
     */
    protected int getMaxTableAliasLength() {
        return MAX_TABLE_ALIAS_LENGTH;
    }
    
    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IJoin)
     */
    public void visit(IJoin obj) {
        final int type = obj.getJoinType();
        if(type != IJoin.CROSS_JOIN) {
            super.visit(obj);
            return;
        }
        
        IFromItem leftItem = obj.getLeftItem();
        if(leftItem instanceof IJoin) {
            buffer.append(LPAREN);
            append(leftItem);
            buffer.append(RPAREN);
        } else {
            append(leftItem);
        }
        buffer.append(SPACE)
            .append(INNER)
            .append(SPACE)
            .append(JOIN)
            .append(SPACE);
        
        IFromItem rightItem = obj.getRightItem();
        if(rightItem instanceof IJoin) {
            buffer.append(LPAREN);
            append(rightItem);
            buffer.append(RPAREN);
        } else {
            append(rightItem);
        }
        
        buffer.append(SPACE)
        .append(ON).append(SPACE)
        .append("1=1");//$NON-NLS-1$
    }
}
