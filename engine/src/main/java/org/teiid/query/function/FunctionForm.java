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

package org.teiid.query.function;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.Assertion;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;


/**
 * The FunctionForm class represents a particular form of a function signature.
 * It is different from the FunctionMethod class because it ignores type 
 * information and instead differentiates function signatures based on their
 * function name and the names of the arguments.
 */
public class FunctionForm implements Serializable, Comparable<FunctionForm> {
	private static final long serialVersionUID = 2411783099304320334L;
	
	private String name;
    private String description;
    private String category;
    private List inputParamNames;
    private List inputParamDescs;
    private String outputParamName;
    private String outputParamDesc;
    
    /** 
     * Construct a function form by pulling all info from a FunctionMethod.  Because
     * a FunctionForm is only created from a <b>validated</b> method, we can assume
     * many things, such as that function name, type, category are non-null.
     * @param method FunctionMethod to build form from
     */
    public FunctionForm(FunctionMethod method) { 
        Assertion.isNotNull(method);

        // Get function stuff
        this.name = method.getName().toUpperCase();
        this.description = method.getDescription();
        this.category = method.getCategory().toUpperCase();
        
        // Get input parameter stuff
        List<FunctionParameter> inputParams = method.getInputParameters();
        if(inputParams == null) { 
            inputParamNames = new ArrayList(0);
            inputParamDescs = new ArrayList(0);
        } else {
            inputParamNames = new ArrayList(inputParams.size());
            inputParamDescs = new ArrayList(inputParams.size());
            
            for(int i=0; i<inputParams.size(); i++) { 
                inputParamNames.add(inputParams.get(i).getName().toUpperCase());
                inputParamDescs.add(inputParams.get(i).getDescription());
            }
        }
        
        // Get output parameter stuff
        FunctionParameter outParam = method.getOutputParameter();
        this.outputParamName = outParam.getName().toUpperCase();
        this.outputParamDesc = outParam.getDescription();
    }
    
    /**
     * Get name of function.
     * @return Name
     */
    public String getName() { 
        return this.name;
    }        
    
    /**
     * Get description of function.
     * @return Description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Get category.
     * @return Category
     */
    public String getCategory() {
        return this.category;
    }
    
    /**
     * Get list of argument names.
     * @return List of argument names ({@link java.lang.String})
     */
    public List getArgNames() { 
        return this.inputParamNames;
    }

    /**
     * Get argument name at index.
     * @param index Index to use
     * @return Argument name at index
     */
    public String getArgName(int index) { 
        return (String) this.inputParamNames.get(index);
    }

    /**
     * Get list of argument descriptions.
     * @return List of argument descriptions ({@link java.lang.String})
     */
    public List getArgDescriptions() { 
        return this.inputParamDescs;
    }
    
    /**
     * Get argument description at index.
     * @param index Index to use
     * @return Argument description at index
     */
    public String getArgDescription(int index) { 
        return (String) this.inputParamDescs.get(index);
    }

    /**
     * Get name of return parameter
     * @return Name of return parameter
     */
    public String getReturnName() { 
        return this.outputParamName;
    }
    
    /**
     * Get description of return parameter
     * @return Description of return parameter
     */
    public String getReturnDescription() { 
        return this.outputParamDesc;
    }
    
    /**
     * Get display string for this function form     
     * @return Display version of this function form
     */
    public String getDisplayString() { 
        StringBuffer str = new StringBuffer();
        
		if(name.equalsIgnoreCase(FunctionLibrary.CAST)) { 
			str.append(name);
			str.append("("); //$NON-NLS-1$
			str.append(inputParamNames.get(0));
			if(name.equalsIgnoreCase(FunctionLibrary.CONVERT) || name.equalsIgnoreCase(FunctionLibrary.CAST)) {
				str.append(", "); //$NON-NLS-1$
			} else {
				str.append(" "); //$NON-NLS-1$
				str.append(Reserved.AS);
				str.append(" "); //$NON-NLS-1$
			}	
			str.append(inputParamNames.get(1));
			str.append(")"); //$NON-NLS-1$
										
		} else if(name.equals("+") || name.equals("-") || name.equals("*") || name.equals("/") || name.equals("||")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
			str.append("("); //$NON-NLS-1$
            str.append(inputParamNames.get(0));
            str.append(name);
            str.append(inputParamNames.get(1));
			str.append(")"); //$NON-NLS-1$
		} else {
			str.append(name);
			str.append("("); //$NON-NLS-1$

            if(inputParamNames.size() > 0) { 
                Iterator iter = inputParamNames.iterator();
                str.append(iter.next());
                while(iter.hasNext()) { 
                    str.append(", "); //$NON-NLS-1$
                    str.append(iter.next());
                }
            }
		
			str.append(")"); //$NON-NLS-1$
		}
			
		return str.toString();
    }        

    /** 
     * String representation of the function form for debugging purposes.
     * @return String representation
     */
    public String toString() { 
        return this.getDisplayString();
    }
    
    /**
     * Return hash code based on the name and input parameter names
     * @return Hash code
     */
    public int hashCode() {
        return HashCodeUtil.hashCode(name.hashCode(), inputParamNames.hashCode());   
    }
    
    /**
     * Compare this function form with another based on the name and 
     * argument names.
     * @param obj Other object
     */
    public boolean equals(Object obj) { 
        if(obj == this) { 
            return true;
        } else if(obj == null || !(obj instanceof FunctionForm)) { 
            return false;
        } else {
            FunctionForm other = (FunctionForm) obj;
            return other.getName().equals(getName()) && 
                   other.getArgNames().equals(getArgNames());
        }
    }
    
    /**
     * Implements Comparable interface so that this object can be compared to 
     * other FunctionForm objects and ordered alphabetically.
     * @param obj Other object
     * @return 1 if other > this, 0 if other == this, -1 if other < this
     */
    public int compareTo(FunctionForm other) {
        int compare = this.getName().compareTo( other.getName() );
        if(compare != 0) { 
            return compare;
        }
            
        // Look further into arg names to compare as names are ==
        List otherArgs = other.getArgNames();
        List myArgs = this.getArgNames();
        
        // Compare # of args first
        if(myArgs.size() < otherArgs.size()) { 
            return -1;
        } else if(myArgs.size() > otherArgs.size()) { 
            return 1;
        } // else continue    

        // Same # of args
        for(int i=0; i < myArgs.size(); i++) { 
            compare = ((String)myArgs.get(i)).compareTo( ((String)otherArgs.get(i)) );
            if(compare != 0) { 
                return compare;
            }
        }
        
        // Same
        return 0;                
    }
}
