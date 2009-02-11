/**
 * Field.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class Field  implements java.io.Serializable {
    private boolean autoNumber;

    private int byteLength;

    private boolean calculated;

    private java.lang.String calculatedFormula;

    private boolean caseSensitive;

    private java.lang.String controllerName;

    private boolean createable;

    private boolean custom;

    private java.lang.String defaultValueFormula;

    private boolean defaultedOnCreate;

    private java.lang.Boolean dependentPicklist;

    private int digits;

    private java.lang.Boolean externalId;

    private boolean filterable;

    private java.lang.Boolean htmlFormatted;

    private boolean idLookup;

    private java.lang.String inlineHelpText;

    private java.lang.String label;

    private int length;

    private java.lang.String name;

    private boolean nameField;

    private java.lang.Boolean namePointing;

    private boolean nillable;

    private com.sforce.soap.partner.PicklistEntry[] picklistValues;

    private int precision;

    private java.lang.String[] referenceTo;

    private java.lang.String relationshipName;

    private java.lang.Integer relationshipOrder;

    private boolean restrictedPicklist;

    private int scale;

    private com.sforce.soap.partner.SoapType soapType;

    private java.lang.Boolean sortable;

    private com.sforce.soap.partner.FieldType type;

    private boolean unique;

    private boolean updateable;

    private java.lang.Boolean writeRequiresMasterRead;

    public Field() {
    }

    public Field(
           boolean autoNumber,
           int byteLength,
           boolean calculated,
           java.lang.String calculatedFormula,
           boolean caseSensitive,
           java.lang.String controllerName,
           boolean createable,
           boolean custom,
           java.lang.String defaultValueFormula,
           boolean defaultedOnCreate,
           java.lang.Boolean dependentPicklist,
           int digits,
           java.lang.Boolean externalId,
           boolean filterable,
           java.lang.Boolean htmlFormatted,
           boolean idLookup,
           java.lang.String inlineHelpText,
           java.lang.String label,
           int length,
           java.lang.String name,
           boolean nameField,
           java.lang.Boolean namePointing,
           boolean nillable,
           com.sforce.soap.partner.PicklistEntry[] picklistValues,
           int precision,
           java.lang.String[] referenceTo,
           java.lang.String relationshipName,
           java.lang.Integer relationshipOrder,
           boolean restrictedPicklist,
           int scale,
           com.sforce.soap.partner.SoapType soapType,
           java.lang.Boolean sortable,
           com.sforce.soap.partner.FieldType type,
           boolean unique,
           boolean updateable,
           java.lang.Boolean writeRequiresMasterRead) {
           this.autoNumber = autoNumber;
           this.byteLength = byteLength;
           this.calculated = calculated;
           this.calculatedFormula = calculatedFormula;
           this.caseSensitive = caseSensitive;
           this.controllerName = controllerName;
           this.createable = createable;
           this.custom = custom;
           this.defaultValueFormula = defaultValueFormula;
           this.defaultedOnCreate = defaultedOnCreate;
           this.dependentPicklist = dependentPicklist;
           this.digits = digits;
           this.externalId = externalId;
           this.filterable = filterable;
           this.htmlFormatted = htmlFormatted;
           this.idLookup = idLookup;
           this.inlineHelpText = inlineHelpText;
           this.label = label;
           this.length = length;
           this.name = name;
           this.nameField = nameField;
           this.namePointing = namePointing;
           this.nillable = nillable;
           this.picklistValues = picklistValues;
           this.precision = precision;
           this.referenceTo = referenceTo;
           this.relationshipName = relationshipName;
           this.relationshipOrder = relationshipOrder;
           this.restrictedPicklist = restrictedPicklist;
           this.scale = scale;
           this.soapType = soapType;
           this.sortable = sortable;
           this.type = type;
           this.unique = unique;
           this.updateable = updateable;
           this.writeRequiresMasterRead = writeRequiresMasterRead;
    }


    /**
     * Gets the autoNumber value for this Field.
     * 
     * @return autoNumber
     */
    public boolean isAutoNumber() {
        return autoNumber;
    }


    /**
     * Sets the autoNumber value for this Field.
     * 
     * @param autoNumber
     */
    public void setAutoNumber(boolean autoNumber) {
        this.autoNumber = autoNumber;
    }


    /**
     * Gets the byteLength value for this Field.
     * 
     * @return byteLength
     */
    public int getByteLength() {
        return byteLength;
    }


    /**
     * Sets the byteLength value for this Field.
     * 
     * @param byteLength
     */
    public void setByteLength(int byteLength) {
        this.byteLength = byteLength;
    }


    /**
     * Gets the calculated value for this Field.
     * 
     * @return calculated
     */
    public boolean isCalculated() {
        return calculated;
    }


    /**
     * Sets the calculated value for this Field.
     * 
     * @param calculated
     */
    public void setCalculated(boolean calculated) {
        this.calculated = calculated;
    }


    /**
     * Gets the calculatedFormula value for this Field.
     * 
     * @return calculatedFormula
     */
    public java.lang.String getCalculatedFormula() {
        return calculatedFormula;
    }


    /**
     * Sets the calculatedFormula value for this Field.
     * 
     * @param calculatedFormula
     */
    public void setCalculatedFormula(java.lang.String calculatedFormula) {
        this.calculatedFormula = calculatedFormula;
    }


    /**
     * Gets the caseSensitive value for this Field.
     * 
     * @return caseSensitive
     */
    public boolean isCaseSensitive() {
        return caseSensitive;
    }


    /**
     * Sets the caseSensitive value for this Field.
     * 
     * @param caseSensitive
     */
    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }


    /**
     * Gets the controllerName value for this Field.
     * 
     * @return controllerName
     */
    public java.lang.String getControllerName() {
        return controllerName;
    }


    /**
     * Sets the controllerName value for this Field.
     * 
     * @param controllerName
     */
    public void setControllerName(java.lang.String controllerName) {
        this.controllerName = controllerName;
    }


    /**
     * Gets the createable value for this Field.
     * 
     * @return createable
     */
    public boolean isCreateable() {
        return createable;
    }


    /**
     * Sets the createable value for this Field.
     * 
     * @param createable
     */
    public void setCreateable(boolean createable) {
        this.createable = createable;
    }


    /**
     * Gets the custom value for this Field.
     * 
     * @return custom
     */
    public boolean isCustom() {
        return custom;
    }


    /**
     * Sets the custom value for this Field.
     * 
     * @param custom
     */
    public void setCustom(boolean custom) {
        this.custom = custom;
    }


    /**
     * Gets the defaultValueFormula value for this Field.
     * 
     * @return defaultValueFormula
     */
    public java.lang.String getDefaultValueFormula() {
        return defaultValueFormula;
    }


    /**
     * Sets the defaultValueFormula value for this Field.
     * 
     * @param defaultValueFormula
     */
    public void setDefaultValueFormula(java.lang.String defaultValueFormula) {
        this.defaultValueFormula = defaultValueFormula;
    }


    /**
     * Gets the defaultedOnCreate value for this Field.
     * 
     * @return defaultedOnCreate
     */
    public boolean isDefaultedOnCreate() {
        return defaultedOnCreate;
    }


    /**
     * Sets the defaultedOnCreate value for this Field.
     * 
     * @param defaultedOnCreate
     */
    public void setDefaultedOnCreate(boolean defaultedOnCreate) {
        this.defaultedOnCreate = defaultedOnCreate;
    }


    /**
     * Gets the dependentPicklist value for this Field.
     * 
     * @return dependentPicklist
     */
    public java.lang.Boolean getDependentPicklist() {
        return dependentPicklist;
    }


    /**
     * Sets the dependentPicklist value for this Field.
     * 
     * @param dependentPicklist
     */
    public void setDependentPicklist(java.lang.Boolean dependentPicklist) {
        this.dependentPicklist = dependentPicklist;
    }


    /**
     * Gets the digits value for this Field.
     * 
     * @return digits
     */
    public int getDigits() {
        return digits;
    }


    /**
     * Sets the digits value for this Field.
     * 
     * @param digits
     */
    public void setDigits(int digits) {
        this.digits = digits;
    }


    /**
     * Gets the externalId value for this Field.
     * 
     * @return externalId
     */
    public java.lang.Boolean getExternalId() {
        return externalId;
    }


    /**
     * Sets the externalId value for this Field.
     * 
     * @param externalId
     */
    public void setExternalId(java.lang.Boolean externalId) {
        this.externalId = externalId;
    }


    /**
     * Gets the filterable value for this Field.
     * 
     * @return filterable
     */
    public boolean isFilterable() {
        return filterable;
    }


    /**
     * Sets the filterable value for this Field.
     * 
     * @param filterable
     */
    public void setFilterable(boolean filterable) {
        this.filterable = filterable;
    }


    /**
     * Gets the htmlFormatted value for this Field.
     * 
     * @return htmlFormatted
     */
    public java.lang.Boolean getHtmlFormatted() {
        return htmlFormatted;
    }


    /**
     * Sets the htmlFormatted value for this Field.
     * 
     * @param htmlFormatted
     */
    public void setHtmlFormatted(java.lang.Boolean htmlFormatted) {
        this.htmlFormatted = htmlFormatted;
    }


    /**
     * Gets the idLookup value for this Field.
     * 
     * @return idLookup
     */
    public boolean isIdLookup() {
        return idLookup;
    }


    /**
     * Sets the idLookup value for this Field.
     * 
     * @param idLookup
     */
    public void setIdLookup(boolean idLookup) {
        this.idLookup = idLookup;
    }


    /**
     * Gets the inlineHelpText value for this Field.
     * 
     * @return inlineHelpText
     */
    public java.lang.String getInlineHelpText() {
        return inlineHelpText;
    }


    /**
     * Sets the inlineHelpText value for this Field.
     * 
     * @param inlineHelpText
     */
    public void setInlineHelpText(java.lang.String inlineHelpText) {
        this.inlineHelpText = inlineHelpText;
    }


    /**
     * Gets the label value for this Field.
     * 
     * @return label
     */
    public java.lang.String getLabel() {
        return label;
    }


    /**
     * Sets the label value for this Field.
     * 
     * @param label
     */
    public void setLabel(java.lang.String label) {
        this.label = label;
    }


    /**
     * Gets the length value for this Field.
     * 
     * @return length
     */
    public int getLength() {
        return length;
    }


    /**
     * Sets the length value for this Field.
     * 
     * @param length
     */
    public void setLength(int length) {
        this.length = length;
    }


    /**
     * Gets the name value for this Field.
     * 
     * @return name
     */
    public java.lang.String getName() {
        return name;
    }


    /**
     * Sets the name value for this Field.
     * 
     * @param name
     */
    public void setName(java.lang.String name) {
        this.name = name;
    }


    /**
     * Gets the nameField value for this Field.
     * 
     * @return nameField
     */
    public boolean isNameField() {
        return nameField;
    }


    /**
     * Sets the nameField value for this Field.
     * 
     * @param nameField
     */
    public void setNameField(boolean nameField) {
        this.nameField = nameField;
    }


    /**
     * Gets the namePointing value for this Field.
     * 
     * @return namePointing
     */
    public java.lang.Boolean getNamePointing() {
        return namePointing;
    }


    /**
     * Sets the namePointing value for this Field.
     * 
     * @param namePointing
     */
    public void setNamePointing(java.lang.Boolean namePointing) {
        this.namePointing = namePointing;
    }


    /**
     * Gets the nillable value for this Field.
     * 
     * @return nillable
     */
    public boolean isNillable() {
        return nillable;
    }


    /**
     * Sets the nillable value for this Field.
     * 
     * @param nillable
     */
    public void setNillable(boolean nillable) {
        this.nillable = nillable;
    }


    /**
     * Gets the picklistValues value for this Field.
     * 
     * @return picklistValues
     */
    public com.sforce.soap.partner.PicklistEntry[] getPicklistValues() {
        return picklistValues;
    }


    /**
     * Sets the picklistValues value for this Field.
     * 
     * @param picklistValues
     */
    public void setPicklistValues(com.sforce.soap.partner.PicklistEntry[] picklistValues) {
        this.picklistValues = picklistValues;
    }

    public com.sforce.soap.partner.PicklistEntry getPicklistValues(int i) {
        return this.picklistValues[i];
    }

    public void setPicklistValues(int i, com.sforce.soap.partner.PicklistEntry _value) {
        this.picklistValues[i] = _value;
    }


    /**
     * Gets the precision value for this Field.
     * 
     * @return precision
     */
    public int getPrecision() {
        return precision;
    }


    /**
     * Sets the precision value for this Field.
     * 
     * @param precision
     */
    public void setPrecision(int precision) {
        this.precision = precision;
    }


    /**
     * Gets the referenceTo value for this Field.
     * 
     * @return referenceTo
     */
    public java.lang.String[] getReferenceTo() {
        return referenceTo;
    }


    /**
     * Sets the referenceTo value for this Field.
     * 
     * @param referenceTo
     */
    public void setReferenceTo(java.lang.String[] referenceTo) {
        this.referenceTo = referenceTo;
    }

    public java.lang.String getReferenceTo(int i) {
        return this.referenceTo[i];
    }

    public void setReferenceTo(int i, java.lang.String _value) {
        this.referenceTo[i] = _value;
    }


    /**
     * Gets the relationshipName value for this Field.
     * 
     * @return relationshipName
     */
    public java.lang.String getRelationshipName() {
        return relationshipName;
    }


    /**
     * Sets the relationshipName value for this Field.
     * 
     * @param relationshipName
     */
    public void setRelationshipName(java.lang.String relationshipName) {
        this.relationshipName = relationshipName;
    }


    /**
     * Gets the relationshipOrder value for this Field.
     * 
     * @return relationshipOrder
     */
    public java.lang.Integer getRelationshipOrder() {
        return relationshipOrder;
    }


    /**
     * Sets the relationshipOrder value for this Field.
     * 
     * @param relationshipOrder
     */
    public void setRelationshipOrder(java.lang.Integer relationshipOrder) {
        this.relationshipOrder = relationshipOrder;
    }


    /**
     * Gets the restrictedPicklist value for this Field.
     * 
     * @return restrictedPicklist
     */
    public boolean isRestrictedPicklist() {
        return restrictedPicklist;
    }


    /**
     * Sets the restrictedPicklist value for this Field.
     * 
     * @param restrictedPicklist
     */
    public void setRestrictedPicklist(boolean restrictedPicklist) {
        this.restrictedPicklist = restrictedPicklist;
    }


    /**
     * Gets the scale value for this Field.
     * 
     * @return scale
     */
    public int getScale() {
        return scale;
    }


    /**
     * Sets the scale value for this Field.
     * 
     * @param scale
     */
    public void setScale(int scale) {
        this.scale = scale;
    }


    /**
     * Gets the soapType value for this Field.
     * 
     * @return soapType
     */
    public com.sforce.soap.partner.SoapType getSoapType() {
        return soapType;
    }


    /**
     * Sets the soapType value for this Field.
     * 
     * @param soapType
     */
    public void setSoapType(com.sforce.soap.partner.SoapType soapType) {
        this.soapType = soapType;
    }


    /**
     * Gets the sortable value for this Field.
     * 
     * @return sortable
     */
    public java.lang.Boolean getSortable() {
        return sortable;
    }


    /**
     * Sets the sortable value for this Field.
     * 
     * @param sortable
     */
    public void setSortable(java.lang.Boolean sortable) {
        this.sortable = sortable;
    }


    /**
     * Gets the type value for this Field.
     * 
     * @return type
     */
    public com.sforce.soap.partner.FieldType getType() {
        return type;
    }


    /**
     * Sets the type value for this Field.
     * 
     * @param type
     */
    public void setType(com.sforce.soap.partner.FieldType type) {
        this.type = type;
    }


    /**
     * Gets the unique value for this Field.
     * 
     * @return unique
     */
    public boolean isUnique() {
        return unique;
    }


    /**
     * Sets the unique value for this Field.
     * 
     * @param unique
     */
    public void setUnique(boolean unique) {
        this.unique = unique;
    }


    /**
     * Gets the updateable value for this Field.
     * 
     * @return updateable
     */
    public boolean isUpdateable() {
        return updateable;
    }


    /**
     * Sets the updateable value for this Field.
     * 
     * @param updateable
     */
    public void setUpdateable(boolean updateable) {
        this.updateable = updateable;
    }


    /**
     * Gets the writeRequiresMasterRead value for this Field.
     * 
     * @return writeRequiresMasterRead
     */
    public java.lang.Boolean getWriteRequiresMasterRead() {
        return writeRequiresMasterRead;
    }


    /**
     * Sets the writeRequiresMasterRead value for this Field.
     * 
     * @param writeRequiresMasterRead
     */
    public void setWriteRequiresMasterRead(java.lang.Boolean writeRequiresMasterRead) {
        this.writeRequiresMasterRead = writeRequiresMasterRead;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof Field)) return false;
        Field other = (Field) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.autoNumber == other.isAutoNumber() &&
            this.byteLength == other.getByteLength() &&
            this.calculated == other.isCalculated() &&
            ((this.calculatedFormula==null && other.getCalculatedFormula()==null) || 
             (this.calculatedFormula!=null &&
              this.calculatedFormula.equals(other.getCalculatedFormula()))) &&
            this.caseSensitive == other.isCaseSensitive() &&
            ((this.controllerName==null && other.getControllerName()==null) || 
             (this.controllerName!=null &&
              this.controllerName.equals(other.getControllerName()))) &&
            this.createable == other.isCreateable() &&
            this.custom == other.isCustom() &&
            ((this.defaultValueFormula==null && other.getDefaultValueFormula()==null) || 
             (this.defaultValueFormula!=null &&
              this.defaultValueFormula.equals(other.getDefaultValueFormula()))) &&
            this.defaultedOnCreate == other.isDefaultedOnCreate() &&
            ((this.dependentPicklist==null && other.getDependentPicklist()==null) || 
             (this.dependentPicklist!=null &&
              this.dependentPicklist.equals(other.getDependentPicklist()))) &&
            this.digits == other.getDigits() &&
            ((this.externalId==null && other.getExternalId()==null) || 
             (this.externalId!=null &&
              this.externalId.equals(other.getExternalId()))) &&
            this.filterable == other.isFilterable() &&
            ((this.htmlFormatted==null && other.getHtmlFormatted()==null) || 
             (this.htmlFormatted!=null &&
              this.htmlFormatted.equals(other.getHtmlFormatted()))) &&
            this.idLookup == other.isIdLookup() &&
            ((this.inlineHelpText==null && other.getInlineHelpText()==null) || 
             (this.inlineHelpText!=null &&
              this.inlineHelpText.equals(other.getInlineHelpText()))) &&
            ((this.label==null && other.getLabel()==null) || 
             (this.label!=null &&
              this.label.equals(other.getLabel()))) &&
            this.length == other.getLength() &&
            ((this.name==null && other.getName()==null) || 
             (this.name!=null &&
              this.name.equals(other.getName()))) &&
            this.nameField == other.isNameField() &&
            ((this.namePointing==null && other.getNamePointing()==null) || 
             (this.namePointing!=null &&
              this.namePointing.equals(other.getNamePointing()))) &&
            this.nillable == other.isNillable() &&
            ((this.picklistValues==null && other.getPicklistValues()==null) || 
             (this.picklistValues!=null &&
              java.util.Arrays.equals(this.picklistValues, other.getPicklistValues()))) &&
            this.precision == other.getPrecision() &&
            ((this.referenceTo==null && other.getReferenceTo()==null) || 
             (this.referenceTo!=null &&
              java.util.Arrays.equals(this.referenceTo, other.getReferenceTo()))) &&
            ((this.relationshipName==null && other.getRelationshipName()==null) || 
             (this.relationshipName!=null &&
              this.relationshipName.equals(other.getRelationshipName()))) &&
            ((this.relationshipOrder==null && other.getRelationshipOrder()==null) || 
             (this.relationshipOrder!=null &&
              this.relationshipOrder.equals(other.getRelationshipOrder()))) &&
            this.restrictedPicklist == other.isRestrictedPicklist() &&
            this.scale == other.getScale() &&
            ((this.soapType==null && other.getSoapType()==null) || 
             (this.soapType!=null &&
              this.soapType.equals(other.getSoapType()))) &&
            ((this.sortable==null && other.getSortable()==null) || 
             (this.sortable!=null &&
              this.sortable.equals(other.getSortable()))) &&
            ((this.type==null && other.getType()==null) || 
             (this.type!=null &&
              this.type.equals(other.getType()))) &&
            this.unique == other.isUnique() &&
            this.updateable == other.isUpdateable() &&
            ((this.writeRequiresMasterRead==null && other.getWriteRequiresMasterRead()==null) || 
             (this.writeRequiresMasterRead!=null &&
              this.writeRequiresMasterRead.equals(other.getWriteRequiresMasterRead())));
        __equalsCalc = null;
        return _equals;
    }

    private boolean __hashCodeCalc = false;
    public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = 1;
        _hashCode += (isAutoNumber() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += getByteLength();
        _hashCode += (isCalculated() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getCalculatedFormula() != null) {
            _hashCode += getCalculatedFormula().hashCode();
        }
        _hashCode += (isCaseSensitive() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getControllerName() != null) {
            _hashCode += getControllerName().hashCode();
        }
        _hashCode += (isCreateable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isCustom() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getDefaultValueFormula() != null) {
            _hashCode += getDefaultValueFormula().hashCode();
        }
        _hashCode += (isDefaultedOnCreate() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getDependentPicklist() != null) {
            _hashCode += getDependentPicklist().hashCode();
        }
        _hashCode += getDigits();
        if (getExternalId() != null) {
            _hashCode += getExternalId().hashCode();
        }
        _hashCode += (isFilterable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getHtmlFormatted() != null) {
            _hashCode += getHtmlFormatted().hashCode();
        }
        _hashCode += (isIdLookup() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getInlineHelpText() != null) {
            _hashCode += getInlineHelpText().hashCode();
        }
        if (getLabel() != null) {
            _hashCode += getLabel().hashCode();
        }
        _hashCode += getLength();
        if (getName() != null) {
            _hashCode += getName().hashCode();
        }
        _hashCode += (isNameField() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getNamePointing() != null) {
            _hashCode += getNamePointing().hashCode();
        }
        _hashCode += (isNillable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getPicklistValues() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getPicklistValues());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getPicklistValues(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        _hashCode += getPrecision();
        if (getReferenceTo() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getReferenceTo());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getReferenceTo(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getRelationshipName() != null) {
            _hashCode += getRelationshipName().hashCode();
        }
        if (getRelationshipOrder() != null) {
            _hashCode += getRelationshipOrder().hashCode();
        }
        _hashCode += (isRestrictedPicklist() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += getScale();
        if (getSoapType() != null) {
            _hashCode += getSoapType().hashCode();
        }
        if (getSortable() != null) {
            _hashCode += getSortable().hashCode();
        }
        if (getType() != null) {
            _hashCode += getType().hashCode();
        }
        _hashCode += (isUnique() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isUpdateable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getWriteRequiresMasterRead() != null) {
            _hashCode += getWriteRequiresMasterRead().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(Field.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "Field"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("autoNumber");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "autoNumber"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("byteLength");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "byteLength"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("calculated");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "calculated"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("calculatedFormula");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "calculatedFormula"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("caseSensitive");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "caseSensitive"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("controllerName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "controllerName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("createable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "createable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("custom");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "custom"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("defaultValueFormula");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "defaultValueFormula"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("defaultedOnCreate");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "defaultedOnCreate"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("dependentPicklist");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "dependentPicklist"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("digits");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "digits"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("externalId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "externalId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("filterable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "filterable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("htmlFormatted");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "htmlFormatted"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("idLookup");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "idLookup"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("inlineHelpText");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "inlineHelpText"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("label");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "label"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("length");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "length"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("name");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "name"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("nameField");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "nameField"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("namePointing");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "namePointing"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("nillable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "nillable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("picklistValues");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "picklistValues"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "PicklistEntry"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("precision");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "precision"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("referenceTo");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "referenceTo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("relationshipName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "relationshipName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("relationshipOrder");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "relationshipOrder"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("restrictedPicklist");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "restrictedPicklist"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("scale");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "scale"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("soapType");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "soapType"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "soapType"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sortable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sortable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("type");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "type"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "fieldType"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("unique");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "unique"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("updateable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "updateable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("writeRequiresMasterRead");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "writeRequiresMasterRead"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
    }

    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

    /**
     * Get Custom Serializer
     */
    public static org.apache.axis.encoding.Serializer getSerializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new  org.apache.axis.encoding.ser.BeanSerializer(
            _javaType, _xmlType, typeDesc);
    }

    /**
     * Get Custom Deserializer
     */
    public static org.apache.axis.encoding.Deserializer getDeserializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new  org.apache.axis.encoding.ser.BeanDeserializer(
            _javaType, _xmlType, typeDesc);
    }

}
