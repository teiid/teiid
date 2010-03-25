<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" indent="yes" />
	<xsl:strip-space elements="*" />
	<xsl:template match="VDB">
		<vdb>
			<xsl:attribute name="name"><xsl:value-of select="VDBInfo/Property[@Name='Name']/@Value" /></xsl:attribute>
			<xsl:attribute name="version"><xsl:value-of select="VDBInfo/Property[@Name='Version']/@Value" /></xsl:attribute>
			<xsl:if test="VDBInfo/Property[@Name='Description']">
				<description><xsl:value-of select="VDBInfo/Property[@Name='Description']/@Value" /></description>
			</xsl:if>
			<xsl:for-each select="VDBInfo/Property">
				<xsl:if
					test="@Name!='Name' and @Name!='Version' and @Name!='Description' and @Name!='GUID' and @Name!='VDBArchiveName'">
					<property>
						<xsl:attribute name="name"><xsl:value-of select="@Name" /></xsl:attribute>
						<xsl:attribute name="value"><xsl:value-of select="@Value" /></xsl:attribute>
					</property>
				</xsl:if>
			</xsl:for-each>
			<xsl:for-each select="Model">
				<model>
					<xsl:attribute name="name"><xsl:value-of select="Property[@Name='Name']/@Value" /></xsl:attribute>
					<xsl:if test="Property[@Name='Visibility' and @Value='Private']">
						<xsl:attribute name="visible">false</xsl:attribute>
					</xsl:if>
					<xsl:variable name="name" select="Property[@Name='Name']/@Value" />
					<xsl:if test="//models[starts-with(@name, concat($name, '.'))]">
						<xsl:attribute name="type"><xsl:value-of
							select="//models[starts-with(@name, concat($name, '.'))]/@modelType" /></xsl:attribute>
					</xsl:if>
					<xsl:for-each select="Property">
						<xsl:if test="@Name!='Name' and @Name!='Visibility'">
							<property>
								<xsl:attribute name="name"><xsl:value-of select="@Name" /></xsl:attribute>
								<xsl:attribute name="value"><xsl:value-of select="@Value" /></xsl:attribute>
							</property>
						</xsl:if>
					</xsl:for-each>
					<xsl:for-each select="ConnectorBindings/Connector">
						<source>
							<xsl:attribute name="name"><xsl:value-of select="@Name" /></xsl:attribute>
							<xsl:attribute name="jndi-name"><xsl:value-of select="concat('java:',translate(@Name, ' ', '_'))" /></xsl:attribute>
						</source>
					</xsl:for-each>
				</model>
			</xsl:for-each>
		</vdb>
	</xsl:template>
</xsl:stylesheet>