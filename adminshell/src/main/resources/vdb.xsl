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
						    <xsl:variable name="connector-name" select="@Name"/>
							<xsl:attribute name="name"><xsl:value-of select="@Name" /></xsl:attribute>
                            <xsl:attribute name="connection-jndi-name"><xsl:value-of select="concat('java:',translate(@Name, ' ', '_'))" /></xsl:attribute>

							<xsl:for-each select="//ConnectorBindings/Connector">
							    <xsl:if test="@Name=$connector-name">
                                    <xsl:choose>
                                        <xsl:when test="@ComponentType='Oracle Connector'">
                                            <xsl:attribute name="translator-name">oracle</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='Oracle XA Connector'">
                                            <xsl:attribute name="translator-name">oracle</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='DB2 Connector'">
                                            <xsl:attribute name="translator-name">db2</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='DB2 XA Connector'">
                                            <xsl:attribute name="translator-name">db2</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='SQL Server Connector'">
                                            <xsl:attribute name="translator-name">sqlserver</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='SQL Server XA Connector'">
                                            <xsl:attribute name="translator-name">sqlserver</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='MySQL JDBC Connector'">
                                            <xsl:attribute name="translator-name">mysql</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='MySQL 5 JDBC Connector'">
                                            <xsl:attribute name="translator-name">mysql</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='MySQL JDBC XA Connector'">
                                            <xsl:attribute name="translator-name">mysql5</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='MySQL 5 JDBC XA Connector'">
                                            <xsl:attribute name="translator-name">mysql5</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='PostgreSQL JDBC Connector'">
                                            <xsl:attribute name="translator-name">postgresql</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='PostgreSQL XA JDBC Connector'">
                                            <xsl:attribute name="translator-name">postgresql</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='Apache Derby Embedded Connector'">
                                            <xsl:attribute name="translator-name">derby</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='Apache Derby Network Connector'">
                                            <xsl:attribute name="translator-name">derby</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='Apache Derby XA Network Connector'">
                                            <xsl:attribute name="translator-name">derby</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='Teiid 6 JDBC Connector'">
                                            <xsl:attribute name="translator-name">teiid</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='JDBC ODBC Connector'">
                                            <xsl:attribute name="translator-name">jdbc-ansi</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='MS Access Connector'">
                                            <xsl:attribute name="translator-name">jdbc-ansi</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='MS Excel Connector'">
                                            <xsl:attribute name="translator-name">jdbc-ansi</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='Loopback Connector'">
                                            <xsl:attribute name="translator-name">loopback</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='Salesforce Connector'">
                                            <xsl:attribute name="translator-name">salesforce</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='Text File Connector'">
                                            <xsl:attribute name="translator-name">file</xsl:attribute>
                                        </xsl:when>
                                        <xsl:when test="@ComponentType='XML Connector'">
                                            <xsl:attribute name="translator-name">xml</xsl:attribute>
                                        </xsl:when>                                        
                                        <xsl:when test="@ComponentType='XML File Connector'">
                                            <xsl:attribute name="translator-name">xml</xsl:attribute>
                                        </xsl:when>                                        
                                        <xsl:when test="@ComponentType='XML SOAP Connector'">
                                            <xsl:attribute name="translator-name">xml</xsl:attribute>
                                        </xsl:when>                                        
                                        <xsl:when test="@ComponentType='XML-Relational File Connector'">
                                            <xsl:attribute name="translator-name">xml</xsl:attribute>
                                        </xsl:when>                                        
                                        <xsl:when test="@ComponentType='XML-Relational HTTP Connector'">
                                            <xsl:attribute name="translator-name">xml</xsl:attribute>
                                        </xsl:when>                                        
                                        <xsl:when test="@ComponentType='XML-Relational SOAP Connector'">
                                            <xsl:attribute name="translator-name">xml</xsl:attribute>
                                        </xsl:when>                                        
                                        <xsl:otherwise>
                                            <xsl:attribute name="translator-name"><xsl:value-of select="translate(substring-before(@ComponentType, ' '), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
                                        </xsl:attribute>
                                        </xsl:otherwise>                                        
                                    </xsl:choose>
        							
    							</xsl:if>
							</xsl:for-each>
						</source>
					</xsl:for-each>
				</model>
			</xsl:for-each>
		</vdb>
	</xsl:template>
</xsl:stylesheet>