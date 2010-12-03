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

package org.teiid.metadata;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAttribute;


/**
 * A function parameter defines the name and description of an input or output
 * parameter for a function.  The name should not be null, but that is not 
 * verified by this class.  Validation of this object can be done with the 
 * {@link FunctionMetadataValidator}.  The type string used in the function
 * parameter should be one of the standard type names defined in 
 * {@link org.teiid.core.types.DataTypeManager.DefaultDataTypes}.
 */
public class FunctionParameter implements Serializable {
	private static final long serialVersionUID = -4696050948395485266L;

	public static final String OUTPUT_PARAMETER_NAME = "result"; //$NON-NLS-1$
	
    private String name;  
    private String type;
    private String description;
    private boolean isVarArg;

    /**
     * Construct a function parameter with no attributes.
     */
    public FunctionParameter() {
    }

    /**
     * Construct a function parameter with no description.
     * @param name Name
     * @param type Type from standard set of types
     */
    public FunctionParameter(String name, String type) {
        this(name, type, null);
    }
    
    /**
     * Construct a function parameter with all attributes.  
     * @param name Name
     * @param type Type from standard set of types
     * @param description Description
     */
    public FunctionParameter(String name, String type, String description) { 
        this(name, type, description, false);
    }
    
    public FunctionParameter(String name, String type, String description, boolean vararg) { 
        setName(name);
        setType(type);
        setDescription(description);
        this.isVarArg = vararg;
    }
    
    /**
     * Return name of parameter.
     * @return Name
     */
    @XmlAttribute
    public String getName() {
        return this.name;
    }
    
    /**
     * Set name
     * @param name Name
     */
    public void setName(String name) { 
        this.name = name;
    }
    
    /**
     * Get description of parameter
     * @return Description
     */
    @XmlAttribute
    public String getDescription() { 
        return this.description;
    }        
    
    /**
     * Set description of parameter
     * @param description Description
     */
    public void setDescription(String description) { 
        this.description = description;
    }
       
    /**
     * Get type of parameter
     * @return Type name
     * @see org.teiid.core.types.DataTypeManager.DefaultDataTypes
     */
    @XmlAttribute
    public String getType() { 
        return this.type;
    }        
    
    /**
     * Set type of parameter
     * @param type Type of parameter
     * @see org.teiid.core.types.DataTypeManager.DefaultDataTypes
     */
    public void setType(String type) {
        if(type == null) { 
            this.type = null;
        } else {
            this.type = type.toLowerCase();
        } 
    }
       
    /**
     * Return hash code for this parameter.  The hash code is based only 
     * on the type of the parameter.  Changing the type of the parameter 
     * after placing this object in a hashed collection will likely cause
     * the object to be lost.
     * @return Hash code
     */   
    public int hashCode() { 
        if(this.type == null) { 
            return 0;
        }
        return this.type.hashCode();
    }
    
    /**
     * Compare with other object for equality.  Equality is based on whether
     * the type is the same as the other parameter.
     * @return True if equal to obj
     */   
    public boolean equals(Object obj) {
        if(obj == this) { 
            return true;
        } 
        if(!(obj instanceof FunctionParameter)) {
        	return false;
        }
        FunctionParameter other = (FunctionParameter) obj;
        if(other.getType() == null) { 
            return (this.getType() == null);
        }
        return other.getType().equals(this.getType()) && this.isVarArg == other.isVarArg;
    }
       
    /**
     * Return string version for debugging purposes
     * @return String representation of function parameter
     */ 
    public String toString() { 
        return type + (isVarArg?"... ":" ") + name; //$NON-NLS-1$ //$NON-NLS-2$
    }

	public void setVarArg(boolean isVarArg) {
		this.isVarArg = isVarArg;
	}

	public boolean isVarArg() {
		return isVarArg;
	}
        
}
