<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:mm="http://www.metamatrix.com/metabase/3.0/metamodels/XMLSchema.xml" xmlns:sdt="http://www.metamatrix.com/metabase/3.0/metamodels/SimpleDatatypes.xml" xmlns:conn="http://www.metamatrix.com/metabase/3.0/metamodels/Connections.xml" xmlns:mmtns="tns" xmlns:dgm="http://www.metamatrix.com/metabase/3.0/metamodels/Diagram.xml" version="1.0">
	<xsl:output method="xml" version="1.0" encoding="ISO-8859-1" omit-xml-declaration="no" indent="yes" media-type="text/html"/>

	<!--
  ************************************************************************
  ** Primary template
  ************************************************************************ -->
	<xsl:template match="/">
		<xsl:choose>
			<xsl:when test="boolean(//sdt:Domain)">
				<xsl:for-each select="//sdt:Domain">
					<xsl:call-template name="document"/>
				</xsl:for-each>
			</xsl:when>
			<xsl:otherwise>
				<xsl:apply-templates select="//mm:SchemaDocument"/>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>

	<!--
  ************************************************************************
  ** Skip diagram and connection information
  ************************************************************************ -->
	<xsl:template match="conn:Connections"/>
	<xsl:template match="dgm:PackageDiagram"/>

	<!--
  ************************************************************************
  ** Process document root
  ************************************************************************ -->
	<xsl:template match="//mm:SchemaDocument" name="document">
		<xsd:schema>
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:schema'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsd:schema>
	</xsl:template>
	
	<!--
  ************************************************************************
  ** Process annotation
  ************************************************************************ -->
	<xsl:template match="mm:Annotation">
		<xsl:if test="not(local-name(../..)='AtomicType') and not(local-name(../..)='ListType') and not(local-name(../..)='UnionType')">
			<xsl:element name="xsd:annotation">
				<xsl:apply-templates select="./@*">
					<xsl:with-param name="parentName" select="'xsd:annotation'"/>
				</xsl:apply-templates>
				<xsl:apply-templates select="./*"/>
			</xsl:element>
		</xsl:if>
	</xsl:template>
	<!--
  ************************************************************************
  ** Process applicationInfo 
  ************************************************************************ -->
	<xsl:template match="mm:ApplicationInfo">
		<xsl:element name="xsd:appinfo">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:appinfo'"/>
			</xsl:apply-templates>
			<xsl:for-each select="@description">
				<xsl:value-of select="."/>
			</xsl:for-each>
			<xsl:value-of select="./@content"/>
		</xsl:element>
	</xsl:template>
	<!--
  ************************************************************************
  ** Process documentation 
  ************************************************************************ -->
	<xsl:template match="mm:Documentation">
		<xsl:element name="xsd:documentation">
			<xsl:value-of select="./mm:Documentation.description"></xsl:value-of>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process Complex Type
  ************************************************************************ -->
	<xsl:template match="mm:ComplexType">
		<xsl:element name="xsd:complexType">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:complexType'"/>
			</xsl:apply-templates>
			<xsl:variable name="contentTag">
				<xsl:choose>
					<xsl:when test="./@mixed='true' or boolean(.//*)">
						<xsl:value-of select="'xsd:complexContent'"/>
					</xsl:when>
					<xsl:otherwise>
						<xsl:value-of select="'xsd:simpleContent'"/>
					</xsl:otherwise>
				</xsl:choose>
			</xsl:variable>
			<xsl:variable name="derivationMethodTag">
				<xsl:choose>
					<xsl:when test="./@derivationMethod='extension'">
						<xsl:value-of select="'xsd:extension'"/>
					</xsl:when>
					<xsl:otherwise>
						<xsl:value-of select="'xsd:restriction'"/>
					</xsl:otherwise>
				</xsl:choose>
			</xsl:variable>
			<xsl:element name="{$contentTag}">
				<xsl:if test="./@mixed='true' ">
					<xsl:attribute name="mixed">
						<xsl:value-of select="'true'"/>
					</xsl:attribute>
				</xsl:if>
				<xsl:variable name="uuid">
					<xsl:value-of select="./@baseType"/>
				</xsl:variable>
				<xsl:element name="{$derivationMethodTag}">
					<xsl:choose>
						<xsl:when test="boolean(@baseType)">
							<xsl:call-template name="BtnUuidToName">
								<xsl:with-param name="uuid" select="$uuid"/>
								<xsl:with-param name="localName" select="'base'"/>
							</xsl:call-template>
						</xsl:when>
						<xsl:otherwise>
							<xsl:attribute name="base">xsd:anyType</xsl:attribute>
						</xsl:otherwise>
					</xsl:choose>
					<xsl:apply-templates select="./*/*" />	
				</xsl:element>
			</xsl:element>		
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process Atomic Data Type
 ************************************************************************ -->
	<xsl:template match="mm:AtomicType">
		<xsl:element name="xsd:simpleType">
			<xsl:for-each select="./@*">
				<xsl:apply-templates select=".">
					<xsl:with-param name="parentName" select="'xsd:simpleType'"/>
				</xsl:apply-templates>
			</xsl:for-each>	
			<xsl:for-each select="./*/mm:Annotation">
				<xsl:element name="xsd:annotation">
					<xsl:apply-templates select="./*"/>
				</xsl:element>
			</xsl:for-each>	
			<xsl:for-each select="./@baseType">
				<xsl:call-template name="sdtAttribute"/>
			</xsl:for-each>					
			<xsl:apply-templates select="./*/*"/>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process List / Union
 ************************************************************************ -->
	<!-- list component -->
	<xsl:template match="mm:ListType">
		<xsl:element name="xsd:simpleType">
			<xsl:for-each select="./@*">
				<xsl:apply-templates select=".">
					<xsl:with-param name="parentName" select="'xsd:simpleType'"/>
				</xsl:apply-templates>
			</xsl:for-each>
			<xsl:for-each select="./*/mm:Annotation">
				<xsl:element name="xsd:annotation">
					<xsl:apply-templates select="./*/*"/>
				</xsl:element>
			</xsl:for-each>	
			<xsl:element name="xsd:list">
				<xsl:if test="boolean(@itemType)">
					<xsl:for-each select="@itemType">
						<xsl:call-template name="BtnUuidToName">
							<xsl:with-param name="uuid" select="."/>
							<xsl:with-param name="localName" select=" 'itemType' "/>
						</xsl:call-template>
					</xsl:for-each>
				</xsl:if>					
				<xsl:apply-templates select="./*"/>
			</xsl:element>
		</xsl:element>
	</xsl:template>
	
	<!-- union component -->
	<xsl:template match="mm:UnionType">
		<xsl:element name="xsd:simpleType">
			<xsl:for-each select="./@*">
				<xsl:apply-templates select=".">
					<xsl:with-param name="parentName" select="'xsd:simpleType'"/>
				</xsl:apply-templates>
			</xsl:for-each>
			<xsl:for-each select="./*/mm:Annotation">
				<xsl:element name="xsd:annotation">
					<xsl:apply-templates select="./*"/>
				</xsl:element>
			</xsl:for-each>
			<xsl:element name="xsd:union">
				<xsl:if test="boolean(@memberTypes)">
					<xsl:for-each select="@memberTypes">
						<xsl:call-template name="BtnUuidToName">
							<xsl:with-param name="uuid" select="."/>
							<xsl:with-param name="localName" select=" 'memberTypes' "/>
						</xsl:call-template>
					</xsl:for-each>
				</xsl:if>
				<xsl:apply-templates select="./*/*"/>
			</xsl:element>
		</xsl:element>
	</xsl:template>
	
	<!--
 ************************************************************************
  ** Process Element
  ************************************************************************ -->
	<xsl:template match="mm:Element">
		<xsl:element name="xsd:element">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:element'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	
	<!--
 ************************************************************************
  ** Process Notation
  ************************************************************************ -->
	<xsl:template match="mm:Notation">
		<xsl:element name="xsd:notation">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:notation'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>

	<!--
 ************************************************************************
  ** Process Sequence
  ************************************************************************ -->
	<xsl:template match="mm:Sequence">
		<xsl:element name="xsd:sequence">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:sequence'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
		
	<!--
 ************************************************************************
  ** Process All
  ************************************************************************ -->
	<xsl:template match="mm:All">
		<xsl:element name="xsd:all">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:all'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>

	<!--
 ************************************************************************
  ** Process Choice
  ************************************************************************ -->
	<xsl:template match="mm:Choice">
		<xsl:element name="xsd:choice">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:choice'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*/*"/>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process Include
 ************************************************************************ -->
	<xsl:template match="mm:Include">
		<xsl:element name="xsd:include">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:include'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process Group
 ************************************************************************ -->
	<xsl:template match="mm:Group">
		<xsl:element name="xsd:group">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:group'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process AttributeGroup
 ************************************************************************ -->
	<xsl:template match="mm:AttributeGroup">
		<xsl:element name="xsd:attributeGroup">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:attributeGroup'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process Any
 ************************************************************************ -->
	<xsl:template match="mm:Any">
		<xsl:element name="xsd:any">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:any'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	
<!--
 ************************************************************************
  ** Process Attribute Element
 ************************************************************************ -->
	<xsl:template match="mm:Attribute">
		<xsl:element name="xsd:attribute">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:attribute'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	
	<!--	
 ************************************************************************
  ** Process Enumeration
 ************************************************************************ -->
	<xsl:template name="enumeration">
		<xsl:element name="xsd:enumeration">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:enumeration'"/>
			</xsl:apply-templates>
			<xsl:for-each select="@description">
				<xsl:element name="xsd:annotation">
					<xsl:element name="xsd:documentation">
						<xsl:value-of select="."/>
					</xsl:element>
				</xsl:element>
			</xsl:for-each>
			<xsl:for-each select="./*">
				<xsl:choose>
					<xsl:when test="local-name()='Enumeration.value' ">
						<xsl:attribute name="value">
						 	<xsl:value-of select="."/>
						</xsl:attribute>
					</xsl:when>
					<xsl:when test="local-name()='Enumeration.description' ">
						<xsl:element name="xsd:annotation">
							<xsl:element name="xsd:documentation">
								<xsl:value-of select="."/>
							</xsl:element>
						</xsl:element>					
					</xsl:when>
				</xsl:choose>
			</xsl:for-each>
		</xsl:element>
	</xsl:template>
	
	<!--	
 ************************************************************************
  ** Process Pattern
 ************************************************************************ -->
	<xsl:template name="pattern">
		<xsl:element name="xsd:pattern">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:pattern'"/>
			</xsl:apply-templates>
			<xsl:for-each select="@description">
				<xsl:element name="xsd:annotation">
					<xsl:element name="xsd:documentation">
						<xsl:value-of select="."/>
					</xsl:element>
				</xsl:element>
			</xsl:for-each>
			<xsl:for-each select="./*">
				<xsl:choose>
					<xsl:when test="local-name()='Pattern.value' ">
						<xsl:attribute name="value">
						 	<xsl:value-of select="."/>
						</xsl:attribute>
					</xsl:when>
					<xsl:when test="local-name()='Pattern.description' ">
						<xsl:element name="xsd:annotation">
							<xsl:element name="xsd:documentation">
								<xsl:value-of select="."/>
							</xsl:element>
						</xsl:element>					
					</xsl:when>
				</xsl:choose>
			</xsl:for-each>
		</xsl:element>
	</xsl:template>
	
	
	<xsl:template match="mm:Enumeration.description">
	</xsl:template>
	<xsl:template match="mm:Enumeration.value">
	</xsl:template>
	<xsl:template match="mm:Pattern.description">
	</xsl:template>
	<xsl:template match="mm:Pattern.value">
	</xsl:template>
	<xsl:template match="mm:Element.constraintValue"/>
	<xsl:template match="mm:Attribute.constraintValue"/>

	<!--
 ************************************************************************
  ** Process AnyAttribute
 ************************************************************************ -->
	<xsl:template match="mm:AnyAttribute">
		<xsl:element name="xsd:anyAttribute">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:anyAttribute'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process Import
 ************************************************************************ -->
	<xsl:template match="mm:Import">
		<xsl:element name="xsd:import">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:import'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process Redefine
 ************************************************************************ -->
	<xsl:template match="mm:Redefine">
		<xsl:element name="xsd:redefine">
			<xsl:apply-templates select="./@*">
				<xsl:with-param name="parentName" select="'xsd:redefine'"/>
			</xsl:apply-templates>
			<xsl:apply-templates select="./*"/>
		</xsl:element>
	</xsl:template>
	<!--
 ************************************************************************
  ** Process simple data type attributes
 ************************************************************************ -->
	<xsl:template name="sdtAttribute">
		<xsl:variable name="localName">
			<xsl:choose>
				<xsl:when test="local-name()='baseType'">
					<xsl:value-of select="'base'"/>
				</xsl:when>
				<xsl:otherwise>
					<xsl:value-of select="local-name()"/>
				</xsl:otherwise>
			</xsl:choose>
		</xsl:variable>
		<xsl:variable name="value">
			<xsl:value-of select="."/>
		</xsl:variable>
		<xsl:choose>		
			<xsl:when test="$localName='base'">
				<xsl:element name="xsd:restriction">
					<xsl:if test="boolean(../@baseType)">
						<xsl:call-template name="BtnUuidToName">
							<xsl:with-param name="uuid" select="../@baseType"/>
							<xsl:with-param name="localName" select="$localName"/>
						</xsl:call-template>
					</xsl:if>
					<xsl:if test="boolean(../@fractionDigits)">
						<xsl:element name="xsd:fractionDigits">
							<xsl:attribute name="value"><xsl:value-of select="../@fractionDigits"/></xsl:attribute>
							<xsl:for-each select="../@fractionDigitsDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@fractionDigitsFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@fractionDigitsFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@whitespace)">
						<xsl:element name="xsd:whiteSpace">
							<xsl:attribute name="value"><xsl:value-of select="../@whitespace"/></xsl:attribute>
							<xsl:for-each select="../@whitespaceDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@whitespaceFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@whitespaceFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@length)">
						<xsl:element name="xsd:length">
							<xsl:attribute name="value"><xsl:value-of select="../@length"/></xsl:attribute>
							<xsl:for-each select="../@lengthDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@lengthFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@lengthFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@maxLength)">
						<xsl:element name="xsd:maxLength">
							<xsl:attribute name="value"><xsl:value-of select="../@maxLength"/></xsl:attribute>
							<xsl:for-each select="../@maxLengthDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@maxLengthFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@maxLengthFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@minLength)">
						<xsl:element name="xsd:minLength">
							<xsl:attribute name="value"><xsl:value-of select="../@minLength"/></xsl:attribute>
							<xsl:for-each select="../@minLengthDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@minLengthFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@minLengthFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@minInclusive)">
						<xsl:element name="xsd:minInclusive">
							<xsl:attribute name="value"><xsl:value-of select="../@minInclusive"/></xsl:attribute>
							<xsl:for-each select="../@minInclusiveDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@minInclusiveFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@minInclusiveFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@maxInclusive)">
						<xsl:element name="xsd:maxInclusive">
							<xsl:attribute name="value"><xsl:value-of select="../@maxInclusive"/></xsl:attribute>
							<xsl:for-each select="../@maxInclusiveDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@maxInclusiveFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@maxInclusiveFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@minExclusive)">
						<xsl:element name="xsd:minExclusive">
							<xsl:attribute name="value"><xsl:value-of select="../@minExclusive"/></xsl:attribute>
							<xsl:for-each select="../@minExclusiveDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@minExclusiveFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@minExclusiveFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@maxExclusive)">
						<xsl:element name="xsd:maxExclusive">
							<xsl:attribute name="value"><xsl:value-of select="../@maxExclusive"/></xsl:attribute>
							<xsl:for-each select="../@maxExclusiveDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@maxExclusiveFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@maxExclusiveFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:if test="boolean(../@totalDigits)">
						<xsl:element name="xsd:totalDigits">
							<xsl:attribute name="value"><xsl:value-of select="../@totalDigits"/></xsl:attribute>
							<xsl:for-each select="../@totalDigitsDescription">
								<xsl:element name="xsd:annotation">
									<xsl:element name="xsd:documentation">
										<xsl:value-of select="."/>
									</xsl:element>
								</xsl:element>
							</xsl:for-each>
							<xsl:if test="boolean(../@totalDigitsFixed)">
								<xsl:attribute name="fixed"><xsl:value-of select="../@totalDigitsFixed"/></xsl:attribute>
							</xsl:if>
						</xsl:element>
					</xsl:if>
					<xsl:for-each select="../*/mm:Enumeration">
						<xsl:call-template name="enumeration"/>
					</xsl:for-each>
					<xsl:for-each select="../*/mm:Pattern">
						<xsl:call-template name="pattern"/>	
					</xsl:for-each>
				</xsl:element>
			</xsl:when>
			<xsl:when test="$localName='xmi.uuid'">
				<xsl:attribute name="id"><xsl:value-of select="."/></xsl:attribute>
			</xsl:when>
			<xsl:when test="$localName='derivationMethod'"/>
			<xsl:when test="$localName='runtimeDataType'"/>
			<xsl:when test="$localName='variety'"/>
			<xsl:when test="$localName='xmi.id'"/>
			<xsl:otherwise>
				<xsl:attribute name="{local-name()}">
					<xsl:value-of select="."/>
				</xsl:attribute>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>
	
	
	<xsl:template match="*/Enumeration.description">
	xxx
	</xsl:template>
	<xsl:template match="*/Enumeration.value">
	xxx
	</xsl:template>
	<xsl:template match="*/Pattern.description">
	xxx
	</xsl:template>
	<xsl:template match="*/Pattern.value">
	xxx
	</xsl:template>
		
	<!--
 ************************************************************************
  ** Process Other Attributes and add references where required
 ************************************************************************ -->
	<xsl:template match="*/@*">
		<xsl:param name="parentName" select="concat('xsd:', ../@name)"/>
		<xsl:variable name="localName">
			<xsl:choose>
				<xsl:when test="local-name()='baseType'">
					<xsl:value-of select="'base'"/>
				</xsl:when>
				<xsl:when test="local-name()='reference'">
					<xsl:value-of select="'ref'"/>
				</xsl:when>
				<xsl:otherwise>
					<xsl:value-of select="local-name()"/>
				</xsl:otherwise>
			</xsl:choose>
		</xsl:variable>
		<xsl:variable name="childTagName">
			<xsl:value-of select="concat($parentName, '.', $localName)"/>
		</xsl:variable>
		<xsl:variable name="value">
			<xsl:value-of select="."/>
		</xsl:variable>
		<xsl:variable name="idRef">
			<xsl:value-of select="//*/@name [./@xmi.uuid = $value]"/>
		</xsl:variable>
		<xsl:choose>
			<xsl:when test="$parentName='xsd:documentation' and $localName='content'">
				<xsl:value-of select="."/>
			</xsl:when>
			<xsl:when test="local-name()='constraint'">
				<xsl:choose>
					<xsl:when test="boolean(../@constraintValue)">
						<xsl:attribute name="{$value}"><xsl:value-of select="../@constraintValue"/></xsl:attribute>
					</xsl:when>
					<xsl:otherwise>
						<xsl:attribute name="{$value}"><xsl:value-of select="../*"/></xsl:attribute>
					</xsl:otherwise>
				</xsl:choose>
			</xsl:when>
			<xsl:when test="local-name()='use' and (starts-with(local-name(../..), 'SchemaDocument.contents') or starts-with(local-name(../..), 'Domain.contents'))"></xsl:when>
			<xsl:when test="local-name()='multiplicity' "></xsl:when>
			<xsl:when test="local-name()='anonymous' "></xsl:when>
			<xsl:when test="local-name()='bounded' "></xsl:when>
			<xsl:when test="local-name()='changeability' "></xsl:when>
			<xsl:when test="local-name()='memberTypes' "></xsl:when>
			<xsl:when test="local-name()='numeric' "></xsl:when>
			<xsl:when test="local-name()='itemType' "></xsl:when>
			<xsl:when test="local-name()='xmi.uuid' and local-name(..)='Documentation'"></xsl:when>
			<xsl:when test="local-name()='xmi.uuid' and local-name(..)='ApplicationInfo'"></xsl:when>
			<xsl:when test="local-name()='mixed' and local-name(..)='ComplexType'"></xsl:when>
			<xsl:when test="local-name()='abstract' and local-name(..)='Element'"></xsl:when>
			<xsl:when test="local-name()='abstract' and $value='false'"></xsl:when>
			<xsl:when test="local-name()='alias' and local-name(..)='Sequence'"></xsl:when>
			<xsl:when test="local-name()='alias' and local-name(..)='Choice'"></xsl:when>
			<xsl:when test="local-name()='alias' and local-name(..)='All'"></xsl:when>
			<xsl:when test="local-name()='anonymous' and local-name(..)='ComplexType'"></xsl:when>
			<xsl:when test="local-name()='anonymous' and $parentName='xsd:simpleType'"></xsl:when>
			<xsl:when test="local-name()='baseType' and local-name(..)='ComplexType'"></xsl:when>
			<xsl:when test="local-name()='baseType' and $parentName='xsd:simpleType'"/>
			<xsl:when test="local-name()='xmi.uuid' and local-name(..)='Annotation'"></xsl:when>
			<xsl:when test="local-name()='xmi.uuid' and local-name(..)='Group'"></xsl:when>
			<xsl:when test="local-name()='xmi.uuid' and local-name(..)='Sequence'"></xsl:when>
			<xsl:when test="local-name()='xmi.uuid' and local-name(..)='All'"></xsl:when>
			<xsl:when test="local-name()='xmi.uuid' and local-name(..)='Choice'"></xsl:when>
			<xsl:when test="local-name()='xmi.uuid'">
				<xsl:attribute name="id">
					<xsl:value-of select="translate($value, ':', '_')"/>
				</xsl:attribute>
			</xsl:when>
			<xsl:when test="$localName='targetNamespace' and $parentName!='xsd:schema'"/>
			<xsl:when test="$localName='variety' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='runtimeDataType' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='length' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='lengthDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='lengthFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='length' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='pattern' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='patternDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='patternFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxLength' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxLengthDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxLengthFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minLength' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minLengthDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minLengthFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxInclusive' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxInclusiveDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxInclusiveFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minInclusive' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minInclusiveDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minInclusiveFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minExclusive' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minExclusiveDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='minExclusiveFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxExclusive' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxExclusiveDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='maxExclusiveFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='totalDigits' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='totalDigitsDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='totalDigitsFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='fractionDigits' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='fractionDigitsDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='fractionDigitsFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='whitespace' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='whitespaceDescription' and $parentName='xsd:simpleType'"/>
			<xsl:when test="$localName='whitespaceFixed' and $parentName='xsd:simpleType'"/>
			<xsl:when test="local-name()='maxOccurs' and local-name(../..)='SchemaDocument.contents'"/>
			<xsl:when test="local-name()='minOccurs' and local-name(../..)='SchemaDocument.contents'"/>
			<xsl:when test="local-name()='maxOccurs' and $value='1' and ../@minOccurs='1'"/>
			<xsl:when test="local-name()='minOccurs' and $value='1' and ../@maxOccurs='1'"/>
			<xsl:when test="local-name()='description' and local-name(..)='Enumeration' "/>
			<xsl:when test="local-name()='description' and local-name(..)='Pattern' "/>
			<xsl:when test="local-name()='Enumeration.description' "/>
			<xsl:when test="local-name()='Pattern.description' "/>
			<xsl:when test="local-name()='description' and local-name(..)='ApplicationInfo' "/>
			<xsl:when test="local-name()='name' and local-name(..)='Sequence'"/>
			<xsl:when test="local-name()='name' and local-name(..)='All'"/>
			<xsl:when test="local-name()='name' and local-name(..)='Pattern'"/>
			<xsl:when test="local-name()='name' and local-name(..)='Enumeration'"/>
			<xsl:when test="local-name()='name' and local-name(..)='Choice'"/>
			<xsl:when test="local-name()='name' and local-name(..)='Any'"/>
			<xsl:when test="local-name()='name' and local-name(..)='AnyAttribute'"/>
			<xsl:when test="local-name()='name' and boolean(../@reference)"/>
			<xsl:when test="local-name()='name' and local-name(..)='SchemaDocument'"/>
			<xsl:when test="local-name()='name' and local-name(..)='Domain'"/>
			<xsl:when test="local-name()='name' and local-name(..)='Documentation'"/>
			<xsl:when test="local-name()='name' and local-name(..)='ApplicationInfo'"/>
			<xsl:when test="local-name()='name' and local-name(..)='Annotation'"/>			
			<xsl:when test="local-name()='name' and local-name(..)='Import'"/>
			<xsl:when test="local-name()='name' and local-name(..)='Include'"/>
			<xsl:when test="local-name()='name' and $parentName='xsd:simpleType' and not(local-name(../..)='SchemaDocument.contents') and not(local-name(../..)='Domain.contents')  "/>
			<xsl:when test="local-name()='name' and $parentName='xsd:complexType' and not(local-name(../..)='SchemaDocument.contents') and not(local-name(../..)='Domain.contents')  "/>
			<xsl:when test="local-name()='contentType' or local-name()='derivationMethod' or local-name()='idref' or local-name()='constraintValue' or local-name()='xmi.id'"/>
			<xsl:when test="$localName='type' and boolean(.././/* [@xmi.uuid=$value])"/>
			<xsl:when test="$localName='type' or $localName='base' or $localName='ref' or $localName='substitutionGroup' ">
				<xsl:call-template name="BtnUuidToName">
					<xsl:with-param name="uuid" select="$value"/>
					<xsl:with-param name="localName" select="$localName"/>
				</xsl:call-template>
			</xsl:when>
			<xsl:when test="local-name()='namespace' or local-name()='schemaLocation' ">
				<xsl:attribute name="{local-name()}">
					<xsl:value-of select="."/>
				</xsl:attribute>
			</xsl:when>
			<xsl:when test="local-name()='language'">
				<xsl:attribute name="lang" namespace="http://www.w3.org/XML/1998/namespace">
					<xsl:value-of select="."/>
				</xsl:attribute>
			</xsl:when>
			<xsl:otherwise>
				<xsl:attribute name="{local-name()}">
					<xsl:value-of select="."/>
				</xsl:attribute>			
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>
	<!--
 ************************************************************************
  ** Conver Datatype represented by uuid to name
 ************************************************************************ -->
	<xsl:template name="BtnUuidToName">
		<xsl:param name="uuid"/>
		<xsl:param name="localName"/>
		<xsl:choose>
			<xsl:when test="$uuid='mmuuid:bf6c34c0-c442-1e24-9b01-c8207cd53eb7'">
				<xsl:attribute name="{$localName}">xsd:string</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:4ca2ae00-3a95-1e20-921b-eeee28353879'">
				<xsl:attribute name="{$localName}">xsd:NMTOKEN</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:4df43700-3b13-1e20-921b-eeee28353879'">
				<xsl:attribute name="{$localName}">xsd:normalizedString</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:3425cb80-d844-1e20-9027-be6d2c3b8b3a'">
				<xsl:attribute name="{$localName}">xsd:token</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:d4d980c0-e623-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:language</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:e66c4600-e65b-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:Name</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:ac00e000-e676-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:NCName</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:4b0f8500-e6a6-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:NMTOKENS</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:dd33ff40-e6df-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:IDREF</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:88b13dc0-e702-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:ID</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:9fece300-e71a-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:ENTITY</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:3c99f780-e72d-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:IDREFS</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:20360100-e742-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:ENTITIES</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:45da3500-e78f-1e20-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:integer</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:cbdd6e40-b9d2-1e21-8c26-a038c6ed7576'">
				<xsl:attribute name="{$localName}">xsd:nonPositiveInteger</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:0e081200-b8a4-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:nonNegativeInteger</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:86d29280-b8d3-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:negativeInteger</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:8cdee840-b900-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:long</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:33add3c0-b98d-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:int</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:5bbcf140-b9ae-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:short</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:26dc1cc0-b9c8-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:byte</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:1cbbd380-b9ea-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:positiveInteger</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:54b98780-ba14-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:unsignedLong</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:badcbd80-ba63-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:unsignedInt</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:327093c0-ba88-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:unsignedShort</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:cff745c0-baa2-1e21-b812-969c8fc8b016'">
				<xsl:attribute name="{$localName}">xsd:unsignedByte</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:dc476100-c483-1e24-9b01-c8207cd53eb7'">
				<xsl:attribute name="{$localName}">xsd:boolean</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:569dfa00-c456-1e24-9b01-c8207cd53eb7'">
				<xsl:attribute name="{$localName}">xsd:decimal</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:d86b0d00-c48a-1e24-9b01-c8207cd53eb7'">
				<xsl:attribute name="{$localName}">xsd:float</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:1f18b140-c4a3-1e24-9b01-c8207cd53eb7'">
				<xsl:attribute name="{$localName}">xsd:double</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:3b892180-c4a7-1e24-9b01-c8207cd53eb7'">
				<xsl:attribute name="{$localName}">xsd:time</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:65dcde00-c4ab-1e24-9b01-c8207cd53eb7'">
				<xsl:attribute name="{$localName}">xsd:date</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:62472700-a064-1e26-9b08-d6079ebe1f0d'">
				<xsl:attribute name="{$localName}">xsd:char</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:822b9a40-a066-1e26-9b08-d6079ebe1f0d'">
				<xsl:attribute name="{$localName}">xsd:biginteger</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:f2249740-a078-1e26-9b08-d6079ebe1f0d'">
				<xsl:attribute name="{$localName}">xsd:bigdecimal</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:6d9809c0-a07e-1e26-9b08-d6079ebe1f0d'">
				<xsl:attribute name="{$localName}">xsd:timestamp</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:f2249740-a078-1e26-9b08-d6079ebe1f0d'">
				<xsl:attribute name="{$localName}">xsd:object</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:b211a140-f815-1e39-9a38-b027a646f0aa'">
				<xsl:attribute name="{$localName}">xsd:anyType</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:b02c7600-b3f2-1e2a-9a03-beb8638ffd21'">
				<xsl:attribute name="{$localName}">xsd:gYear</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:17d08040-b3ed-1e2a-9a03-beb8638ffd21'">
				<xsl:attribute name="{$localName}">xsd:gYearMonth</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:6e604140-b3f5-1e2a-9a03-beb8638ffd21'">
				<xsl:attribute name="{$localName}">xsd:gMonthDay</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:860b7dc0-b3f8-1e2a-9a03-beb8638ffd21'">
				<xsl:attribute name="{$localName}">xsd:gDay</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:187f5580-b3fb-1e2a-9a03-beb8638ffd21'">
				<xsl:attribute name="{$localName}">xsd:gMonth</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:28d98540-b3e7-1e2a-9a03-beb8638ffd21'">
				<xsl:attribute name="{$localName}">xsd:duration</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:6247ec80-e8a4-1e2a-b433-fb67ea35c07e'">
				<xsl:attribute name="{$localName}">xsd:anyURI</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:3dcaf900-e8dc-1e2a-b433-fb67ea35c07e'">
				<xsl:attribute name="{$localName}">xsd:NOTATION</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:d9998500-ebba-1e2a-9319-8eaa9b2276c7'">
				<xsl:attribute name="{$localName}">xsd:hexBinary</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:b4c99380-ebc6-1e2a-9319-8eaa9b2276c7'">
				<xsl:attribute name="{$localName}">xsd:base64Binary</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:eeb5d780-e8c3-1e2a-b433-fb67ea35c07e'">
				<xsl:attribute name="{$localName}">xsd:QName</xsl:attribute>
			</xsl:when>
			<xsl:when test="$uuid='mmuuid:fd8254c0-27a1-1e43-ad04-8f2cc0176d3f'">
				<xsl:attribute name="{$localName}">xsd:anySimpleType</xsl:attribute>
			</xsl:when>			
			<xsl:when test="$uuid='mmuuid:051a0640-b4e8-1e26-9f33-b76fd9d5fa79'">
				<xsl:attribute name="{$localName}">xsd:bigdecimal</xsl:attribute>
			</xsl:when>
			<xsl:when test="boolean(//@name[../@xmi.uuid=$uuid])">
				<xsl:variable name="tns">
					<xsl:choose>
						<xsl:when test="boolean(//*/@targetNamespace)">
							<xsl:value-of select=" 'mmtns:' "/>
						</xsl:when>
						<xsl:otherwise>
							<xsl:value-of select=" '' "/>
						</xsl:otherwise>
					</xsl:choose>
				</xsl:variable>
				<xsl:attribute name="{$localName}"><xsl:value-of select="concat($tns, //@name[../@xmi.uuid=$uuid])"/></xsl:attribute>
			</xsl:when>
			<xsl:otherwise>
				<xsl:attribute name="{$localName}">
					<xsl:value-of select="$uuid"/>
				</xsl:attribute>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>
</xsl:stylesheet>
