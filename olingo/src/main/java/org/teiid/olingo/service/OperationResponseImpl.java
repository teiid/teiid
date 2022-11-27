package org.teiid.olingo.service;

import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmReturnType;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.odata.api.OperationResponse;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.TeiidODataJsonSerializer;
import org.teiid.olingo.service.ProcedureSQLBuilder.ProcedureReturn;

public class OperationResponseImpl implements OperationResponse {
    private List<ComplexValue> complexValues = new ArrayList<ComplexValue>();
    private Property returnValue;
    private ProcedureReturn procedureReturn;
    private String nextToken;

    public OperationResponseImpl(ProcedureReturn procedureReturn) {
        this.procedureReturn = procedureReturn;
    }

    @Override
    public void addRow(ResultSet rs) throws SQLException {
        this.complexValues.add(getComplexProperty(rs));
    }

    private ComplexValue getComplexProperty(ResultSet rs) throws SQLException {
        HashMap<Integer, Property> properties = new HashMap<Integer, Property>();
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            Object value = rs.getObject(i + 1);
            String propName = rs.getMetaData().getColumnLabel(i + 1);
            EdmElement element = ((EdmComplexType)this.procedureReturn.getReturnType().getType()).getProperty(propName);
            if (!(element instanceof EdmProperty) && !((EdmProperty) element).isPrimitive()) {
                throw new SQLException(new TeiidNotImplementedException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16024)));
            }
            EdmPropertyImpl edmProperty = (EdmPropertyImpl) element;
            Property property;
            try {
                property = EntityCollectionResponse.buildPropery(propName,
                        (SingletonPrimitiveType) edmProperty.getType(), edmProperty.getPrecision(), edmProperty.getScale(), edmProperty.isCollection(), value);
                properties.put(i, property);
            } catch (IOException e) {
                throw new SQLException(e);
            } catch (TeiidProcessingException e) {
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

    @Override
    public long size() {
        return this.complexValues.size();
    }

    @Override
    public void setCount(long count) {
    }

    @Override
    public void setNextToken(String token) {
        this.nextToken = token;
    }

    @Override
    public String getNextToken() {
        return nextToken;
    }

    @Override
    public Property getResult() {
        if (this.procedureReturn.hasResultSet()) {
            String type = this.procedureReturn.getReturnType().getType().getFullQualifiedName().getFullQualifiedNameAsString();
            return createComplexCollection("result", type, this.complexValues);
        }
        return this.returnValue;
    }

    @Override
    public void setReturnValue(Object returnValue) throws SQLException {
        try {
            EdmReturnType returnType = this.procedureReturn.getReturnType();
            this.returnValue = EntityCollectionResponse.buildPropery("return", //$NON-NLS-1$
                    (SingletonPrimitiveType) returnType.getType(), returnType.getPrecision(), returnType.getScale(),
                    returnType.isCollection(), returnValue);
        } catch (TeiidProcessingException e) {
            throw new SQLException(e);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public ProcedureReturn getProcedureReturn() {
        return procedureReturn;
    }

    @Override
    public void serialize(ODataResponse response,
            TeiidODataJsonSerializer serializer, ServiceMetadata metadata,
            ContextURL contextURL, URI next) throws SerializerException {
        response.setContent(serializer.complexCollection(metadata, (EdmComplexType)this.procedureReturn.getReturnType().getType(),
                getResult(), contextURL, next)
                .getContent());
    }
}
