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

package com.metamatrix.common.object;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.core.id.ObjectID;

public final class StandardPropertyTypeValidator implements Serializable {

    private static final PropertyTypeValidator DEFAULT_VALIDATOR = new DefaultValidator();

    public static final Map STANDARD_VALIDATORS = new HashMap();

    static {
        STANDARD_VALIDATORS.put( PropertyType.STRING_NAME,    new StringValidator() );
        STANDARD_VALIDATORS.put( PropertyType.INTEGER_NAME,   new IntegerValidator() );
        STANDARD_VALIDATORS.put( PropertyType.LONG_NAME,      new LongValidator() );
        STANDARD_VALIDATORS.put( PropertyType.FLOAT_NAME,     new FloatValidator() );
        STANDARD_VALIDATORS.put( PropertyType.DOUBLE_NAME,    new DoubleValidator() );
        STANDARD_VALIDATORS.put( PropertyType.BYTE_NAME,      new ByteValidator() );
        STANDARD_VALIDATORS.put( PropertyType.SHORT_NAME,     new ShortValidator() );
        STANDARD_VALIDATORS.put( PropertyType.BOOLEAN_NAME,   new BooleanValidator() );
        STANDARD_VALIDATORS.put( PropertyType.TIME_NAME,      new DefaultValidator() );
        STANDARD_VALIDATORS.put( PropertyType.DATE_NAME,      new DefaultValidator() );
        STANDARD_VALIDATORS.put( PropertyType.TIMESTAMP_NAME, new DefaultValidator() );
        STANDARD_VALIDATORS.put( PropertyType.LIST_NAME,      new DefaultValidator() );
        STANDARD_VALIDATORS.put( PropertyType.SET_NAME,       new DefaultValidator() );
        STANDARD_VALIDATORS.put( PropertyType.URL_NAME,       new URLValidator() );
        STANDARD_VALIDATORS.put( PropertyType.URI_NAME,       new URIValidator() );
        STANDARD_VALIDATORS.put( PropertyType.HOSTNAME_NAME,  new HostnameValidator() );
        STANDARD_VALIDATORS.put( PropertyType.FILE_NAME,      new DirectoryEntryValidator() );
        STANDARD_VALIDATORS.put( PropertyType.OBJECT_ID_NAME, new ObjectIDValidator() );
        STANDARD_VALIDATORS.put( PropertyType.PASSWORD_NAME,       new PasswordValidator() );
        STANDARD_VALIDATORS.put( PropertyType.PROPERTIED_OBJECT_NAME,  new PropertiedObjectValidator() );
        STANDARD_VALIDATORS.put( PropertyType.DESCRIPTOR_NAME,     new DefaultValidator() );
        STANDARD_VALIDATORS.put( PropertyType.OBJECT_REFERENCE_NAME,  new DefaultValidator() );
        STANDARD_VALIDATORS.put( PropertyType.DATA_TYPE_NAME,  new DefaultValidator() );
        STANDARD_VALIDATORS.put( PropertyType.UNBOUNDED_INTEGER_NAME,  new UnboundedIntegerValidator() );
        STANDARD_VALIDATORS.put( PropertyType.REG_EXPRESSION_NAME, new RegularExpressionValidator() );
    }

    public static PropertyTypeValidator lookup( String typeName ) {
        PropertyTypeValidator result = (PropertyTypeValidator) STANDARD_VALIDATORS.get(typeName);
        if ( result == null ) {
            result = DEFAULT_VALIDATOR;
        }
        return result;
    }
    
    /**
    * This helper method will be used to evaluate any PropertyValues that 
    * have a multiplicity of * or be able to have more than one value.
    *
    * We use object arrays because if we used some sort of Collection we would
    * not be able to tell if the Collection was the value or the Collection was
    * the Collection of values for that property as there is a property type of 
    * List.
    *
    * @param values the object array of values to be validated
    * @param the validator instance to use to validate the values in the array
    * @return true if the value is valid for the property type.
    */
    public static boolean isValidValue(Object[] values, PropertyTypeValidator validator) {
        for( int i=0; i<values.length; i++) {
            if (!validator.isValidValue(values[i])) {
                return false;
            }
        }
        // if we have fallen through the loop, then all values are of the proper
        // type:
        return true;
    }

}

// These should always check whether the value is an instanceof a String first, since
// most of the time the value will be a String.

class DefaultValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        return true;
    }
}

class StringValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if (value instanceof String) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class RegularExpressionValidator extends StringValidator {
    public boolean isValidValue(Object value ) {
        if (value instanceof String) {
            // TODO:  Attempt to parse the regular expression
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class PasswordValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( PropertyType.PASSWORD_CLASS.isInstance(value) || (value instanceof String) ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class PropertiedObjectValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof PropertiedObject ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class IntegerValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                Integer.parseInt(value.toString());
            } catch ( NumberFormatException e ) {
                return false;
            }
            return true;
        }else if ( value instanceof Integer ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class LongValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                Long.parseLong(value.toString());
            } catch ( NumberFormatException e ) {
                return false;
            }
            return true;
        }else if ( value instanceof Long ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class ShortValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                Short.parseShort(value.toString());
            } catch ( NumberFormatException e ) {
                return false;
            }
            return true;
        }else if ( value instanceof Short ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class FloatValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                Float.parseFloat(value.toString());
            } catch ( NumberFormatException e ) {
                return false;
            }
            return true;
        }else if ( value instanceof Float ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class DoubleValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                Double.parseDouble(value.toString());
            } catch ( NumberFormatException e ) {
                return false;
            }
            return true;
        }else if ( value instanceof Double ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class ByteValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                Byte.parseByte(value.toString());
            } catch ( NumberFormatException e ) {
                return false;
            }
            return true;
        }else if ( value instanceof Byte ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class BooleanValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
        	if ( ((String) value).equalsIgnoreCase("true") || ((String) value).equalsIgnoreCase("false") ) { //$NON-NLS-1$ //$NON-NLS-2$
        		return true;
        	}
        	return false;
        }else if ( value instanceof Boolean ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class HostnameValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            return true;
        }else if ( value instanceof InetAddress ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class URLValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                new URL(value.toString());
            } catch ( MalformedURLException e ) {
                return false;
            }
            return true;
        }else if ( value instanceof URL ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class URIValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                new URI(value.toString());
            } catch (URISyntaxException e) {
                return false;
            }
            return true;
        }else if ( value instanceof URL ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class DirectoryEntryValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof DirectoryEntry ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class ObjectIDValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof ObjectID ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}
class UnboundedIntegerValidator implements PropertyTypeValidator, Serializable {
    public boolean isValidValue(Object value ) {
        if ( value instanceof String ) {
            try {
                if ( PropertyType.UNBOUNDED_INTEGER_KEYWORD.equals(value) ) {
                    return true;
                }
                Integer.parseInt(value.toString());
            } catch ( NumberFormatException e ) {
                return false;
            }
            return true;
        }else if ( value instanceof Integer ) {
            return true;
        }else if(value instanceof Object[]) {
            return StandardPropertyTypeValidator.isValidValue((Object[])value, this);
        }else {
            return false;
        }
    }
}


