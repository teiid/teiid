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

package com.metamatrix.common.jdbc.metadata;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.core.util.ArgCheck;

abstract public class ColumnSet extends JDBCObject {
    private String catalogName;
    private String schemaName;
    private List columns;
//    private boolean selectable;

    protected ColumnSet() {
        super();
        this.columns = new LinkedList();
    }

    protected ColumnSet(String name) {
        super(name);
        this.columns = new LinkedList();
    }

    public ColumnSet(String catalogName, String schemaName, String name) {
        this(name);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
    }

    public String getCatalogName() {
        return this.catalogName;
    }

    public String getSchemaName() {
        return this.schemaName;
    }


    public List getColumns() {
        return this.columns;
    }

    public void add(Column object) {
        addColumn(object, false);
    }

    public void addAll(Collection columns) {
        for (Iterator it=columns.iterator(); it.hasNext(); ) {
            addColumn( (Column) it.next(), false);
        }
    }

    public boolean remove(Column object) {
        return removeColumn(object, false);
    }

    protected void addColumn(Column object, boolean takeOwnership) {
        if(object == null){
            ArgCheck.isNotNull(object, "The Column reference may not be null"); //$NON-NLS-1$
        }
        
        if (!this.columns.contains(object)) {
            this.columns.add(object);
            if (takeOwnership) {
                object.setOwner(this);
            }
        }
    }

    protected boolean removeColumn(Column object, boolean releaseOwnership) {
        if(object == null){
            ArgCheck.isNotNull(object, "The Column reference may not be null"); //$NON-NLS-1$
        }
        
        if(object.getOwner() != this){
            ArgCheck.isTrue(object.getOwner() == this, "The specified object is not contained by this object"); //$NON-NLS-1$
        }
        
        if (releaseOwnership) {
            object.setOwner(null);
        }
        return this.columns.remove(object);
    }


    public Column lookupColumn(String columnName) {
        return (Column) JDBCObject.lookupJDBCObject(this.getColumns(),columnName,Column.class);
    }


    public boolean contains(Column object) {
        if(object == null){        
            ArgCheck.isNotNull(object, "The Column reference may not be null"); //$NON-NLS-1$
        }
        
        return this.columns.contains(object);
    }

//    public boolean isSelectable() {
//        return selectable;
//    }

//    public void setSelectable(boolean selectable) {
//        this.selectable = selectable;
//    }

    protected String getColumnNames() {
        StringBuffer sb = new StringBuffer();
        Iterator iter = this.columns.iterator();
        if (iter.hasNext()) {
            JDBCObject obj = (JDBCObject)iter.next();
            sb.append(obj.getName());
        }
        while (iter.hasNext()) {
            JDBCObject obj = (JDBCObject)iter.next();
            sb.append(',');
            sb.append(obj.getName());
        }
        return sb.toString();
    }
    
    public boolean hasMatchingColumns( ColumnSet matchingSet ) {
        if ( this.getColumns().size() != matchingSet.getColumns().size() ) {
            return false;    
        }
        
        Iterator thisIter = this.columns.iterator();
        Iterator thatIter = matchingSet.columns.iterator();
        while ( thisIter.hasNext() ) {
            Column thisCol = (Column)thisIter.next();    
            Column thatCol = (Column)thatIter.next();
            if ( thisCol.getName().equals(thatCol.getName()) ) {
                return true;
            }    
        }
        return false;
    }

    boolean generateUniqueName( String prefix ) {
        if ( this.hasName() ) {
            return false;
        }
        
        super.setOriginalNameNull(true);
        
        StringBuffer sb = new StringBuffer();
        if ( prefix != null ) {
            sb.append(prefix);
        }
        String baseName = sb.toString();
        
        // Build the name from the names of the columns ...
//        boolean atLeastOneColumn = false;
        Iterator iter = this.getColumns().iterator();
        if ( iter.hasNext() ) {
            Column column = (Column) iter.next();
            sb.append( column.getName() );
//            atLeastOneColumn = true;
        }
        while ( iter.hasNext() ) {
            Column column = (Column) iter.next();
            sb.append( '_' );
            sb.append( column.getName() );
        }

        String potentialName = null;
        if ( this.hasOwner() ) {
            Table owner = (Table) this.getOwner();
            potentialName = baseName + owner.getName();
            if ( potentialName != null ) {
                baseName = potentialName;
                int counter = 0;
                while ( owner.lookupUniqueKey(potentialName) != null ) {
                    potentialName = baseName + (++counter);
                }
            }
        } else {
//            if ( atLeastOneColumn ) {
                potentialName = sb.toString();
//            } else {
//                sb.append('1');
//                potentialName = sb.toString();
//            }
        }
        this.setName(potentialName);
        return true; 
    }
    
    /**
	 * @since 3.1
	 */
	public String getFullName() {
        final String name = super.getFullName();
        if (name.indexOf(DELIMITER) < 0) {
            final StringBuffer buf = new StringBuffer(0);
            if (this.catalogName != null  &&  this.catalogName.length() > 0) {
                buf.append(this.catalogName);
                buf.append(DELIMITER);
            }
            if (this.schemaName != null  &&  this.schemaName.length() > 0) {
                buf.append(this.schemaName);
                buf.append(DELIMITER);
            }
            buf.append(name);
            return buf.toString();
        }
        return name;
	}
}




