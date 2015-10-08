package org.teiid.olingo.service;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.odata.api.OperationResponse;
import org.teiid.olingo.ODataPlugin;

public class OperationResponseImpl implements OperationResponse {
    private final String invalidCharacterReplacement;
    private EdmType type;
    private List<ComplexValue> complexValues = new ArrayList<ComplexValue>();
    private Object primitiveValue;
    
    public OperationResponseImpl(String invalidCharacterReplacement, EdmType type) {
        this.invalidCharacterReplacement = invalidCharacterReplacement;
        this.type = type;
    }
    
    @Override
    public void addRow(ResultSet rs) throws SQLException {
        // check for special lob case where single value resultset is present
        if (rs.getMetaData().getColumnCount() == 1) {
            Object value = rs.getObject(1);
            if (DataTypeManager.isLOB(value.getClass())) {
                addPrimitive(value);
                return;
            }
        }
        this.complexValues.add(getComplexProperty(rs));
    }
    
    private ComplexValue getComplexProperty(ResultSet rs) throws SQLException {
        HashMap<Integer, Property> properties = new HashMap<Integer, Property>();
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            Object value = rs.getObject(i + 1);
            String propName = rs.getMetaData().getColumnLabel(i + 1);
            EdmElement element = ((EdmComplexType)this.type).getProperty(propName);
            if (!(element instanceof EdmProperty) && !((EdmProperty) element).isPrimitive()) {
                throw new SQLException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16024));
            }
            EdmPropertyImpl edmProperty = (EdmPropertyImpl) element;
            Property property;
            try {
                property = EntityCollectionResponse.buildPropery(propName,
                        (SingletonPrimitiveType) edmProperty.getType(), edmProperty.isCollection(), value,
                        invalidCharacterReplacement);
                properties.put(i, property);
            } catch (IOException e) {
                throw new SQLException(e);
            } catch (TransformationException e) {
                throw new SQLException(e);
            }
        }

        // properties can contain more than what is requested in project to build links
        // filter those columns out.        
        return createComplex("result", properties.values());
    }
    
    static ComplexValue createComplex(final String name, final Collection<Property> properties) {
        ComplexValue complexValue = new ComplexValue();
        for (final Property property : properties) {
            complexValue.getValue().add(property);
        }
        return complexValue;
    }

    static Property createComplexCollection(final String name,
            final String type, final List<ComplexValue> complexList) {
        return new Property(type, name, ValueType.COLLECTION_COMPLEX, complexList);
    }    
    
    static Property createPrimitive(final String name,
            final String type, final Object value) {
        return new Property(type, name, ValueType.PRIMITIVE, value);
    }    

    @Override
    public long size() {
        return this.complexValues.size();
    }

    @Override
    public void setCount(long count) {
    }

    @Override
    public void setNextToken(String token) {
    }

    @Override
    public String getNextToken() {
        return null;
    }

    @Override
    public Object getResult() {
        String type = this.type.getFullQualifiedName().getFullQualifiedNameAsString();        
        if (isReturnTypePrimitive()) {
            return createPrimitive("return", type, this.primitiveValue);
        } else {
            return createComplexCollection("result", type, this.complexValues);
        }
    }
    
    private boolean isReturnTypePrimitive() {
        return type.getKind() == EdmTypeKind.PRIMITIVE;
    }

    @Override
    public void addPrimitive(Object value) {
        this.primitiveValue = value;
    }
}
