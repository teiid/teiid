<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:param name="version">7.0</xsl:param>
	<xsl:output method="xml" indent="yes"/>
	<xsl:strip-space elements="*"/>
	<xsl:template match="VDB">
	<xsl:if test="ConnectorBindings">
	<connection-factories>
	<xsl:for-each select="ConnectorBindings/Connector">
		<no-tx-connection-factory>
		   <jndi-name><xsl:value-of select="translate(@Name, ' ', '_')" /></jndi-name>
		   <xsl:choose>
		   	<xsl:when test="@ComponentType='Text File Connector'">
		   	<rar-name>connector-text-<xsl:value-of select="$version"/>.rar</rar-name>
		   	</xsl:when>
		   	<xsl:when test="starts-with(@ComponentType,'Apache ') or starts-with(@ComponentType,'MySQL ')
		   	   or starts-with(@ComponentType,'Oracle ') or starts-with(@ComponentType,'PostgreSQL ')
		   	   or starts-with(@ComponentType,'SQL Server ') or starts-with(@ComponentType,'DB2 ')
		   	   or starts-with(@ComponentType,'MS Access ') or starts-with(@ComponentType,'MS Excel ')
		   	   or starts-with(@ComponentType,'JDBC ') or starts-with(@ComponentType,'Teiid ')
		   	   or starts-with(@ComponentType,'MM ') or starts-with(@ComponentType,'H2 ')
		   	   or starts-with(@ComponentType,'HSQLDB ') or starts-with(@ComponentType,'Sybase ')
		   	   ">
		   	<rar-name>connector-jdbc-<xsl:value-of select="$version"/>.rar</rar-name>
		   	</xsl:when>
		   	<xsl:when test="@ComponentType='LDAP Connector'">
		   	<rar-name>connector-ldap-<xsl:value-of select="$version"/>.rar</rar-name>
		   	</xsl:when>
		   	<xsl:when test="@ComponentType='Loopback Connector'">
		   	<rar-name>connector-loopback-<xsl:value-of select="$version"/>.rar</rar-name>
		   	</xsl:when>
		   	<xsl:when test="@ComponentType='Salesforce Connector'">
		   	<rar-name>connector-salesforce-<xsl:value-of select="$version"/>.rar</rar-name>
		   	</xsl:when>
		   	<xsl:when test="@ComponentType='XML File Connector' or @ComponentType='XML-Relational File Connector'">
		   	<rar-name>connector-xml-file-<xsl:value-of select="$version"/>.rar</rar-name>
		   	</xsl:when>
		   	<xsl:when test="@ComponentType='XML SOAP Connector' or @ComponentType='XML-Relational Soap Connector'">
		   	<rar-name>connector-xml-soap-<xsl:value-of select="$version"/>.rar</rar-name>
		   	</xsl:when>
		   	<xsl:when test="@ComponentType='XML-Relational HTTP Connector'">
		   	<rar-name>connector-xml-http-<xsl:value-of select="$version"/>.rar</rar-name>
		   	</xsl:when>
		   </xsl:choose>
	                <connection-definition>org.teiid.connector.api.Connector</connection-definition>
		   <xsl:for-each select="Properties/Property">
		   		<xsl:choose>
		   			<xsl:when test="@Name='ConnectorMaxConnections' or @Name='UsePostDelegation'
		   		or @Name='ConnectorThreadTTL' or @Name='DeployedName'
		   		or @Name='ConnectorMaxThreads' or @Name='SetCriteriaBatchSize'
		        or @Name='ConnectorClassPath' or @Name='SourceConnectionTestInterval'
		        or @Name='metamatrix.service.essentialservice' or @Name='ServiceMonitoringEnabled'
		        or @Name='ConnectorClass' or @Name='ServiceClassName'
		        or @Name='SynchWorkers' or @Name='UseCredentialMap'
		        or @Name='ConnectionPoolEnabled' or @Name='AdminConnectionsAllowed'
		        or starts-with(@Name,'com.metamatrix.data.pool') or starts-with(@Name, 'getMax')
		        or starts-with(@Name, 'supports') or starts-with(@Name, 'getSupported')
		        or @Name='requiresCriteria' or @Name='useAnsiJoin'
		        or @Name='URL' or @Name='ConnectionSource'
		        or @Name='User' or @Name='Password' or starts-with(@Name, 'ResultSetCache') 
		        or starts-with(@Name, 'Extension') or @Name='Driver'
		        or @Name='MaxSQLLength'">
		        <xsl:comment>&lt;config-property name="<xsl:value-of select="@Name"/>"&gt;<xsl:value-of select="text()"/>&lt;/config-property&gt;</xsl:comment>
		        	</xsl:when>
		        	<xsl:when test="@Name='MaxResultRows' and text()='0'">
     	        <config-property>
		           <xsl:attribute name="name">
		           	<xsl:value-of select="@Name"/>
		           </xsl:attribute>-1</config-property>		        	
		        	</xsl:when>
		   			<xsl:otherwise>
     	        <config-property>
		           <xsl:attribute name="name">
		           	<xsl:value-of select="@Name"/>
		           </xsl:attribute>
		           <xsl:value-of select="text()"/>
		        </config-property>
		   			</xsl:otherwise>
		   		</xsl:choose>
		   </xsl:for-each>
		   <xsl:if test="starts-with(@ComponentType,'Apache ') or starts-with(@ComponentType,'MySQL ')
		   	   or starts-with(@ComponentType,'Oracle ') or starts-with(@ComponentType,'PostgreSQL ')
		   	   or starts-with(@ComponentType,'SQL Server ') or starts-with(@ComponentType,'DB2 ')
		   	   or starts-with(@ComponentType,'MS Access ') or starts-with(@ComponentType,'MS Excel ')
		   	   or starts-with(@ComponentType,'JDBC ') or starts-with(@ComponentType,'Teiid ')
		   	   or starts-with(@ComponentType,'MM ') or starts-with(@ComponentType,'H2 ')
		   	   or starts-with(@ComponentType,'HSQLDB ') or starts-with(@ComponentType,'Sybase ')
		   	   ">
		   <config-property name="SourceJNDIName">java:<xsl:value-of select="translate(@Name, ' ', '_')" />DS</config-property>
		   <xsl:message>For connector binding "<xsl:value-of select="@Name" />" of legacy type "<xsl:value-of select="@ComponentType" />", you will need to create a -ds.xml JDBC DataSource with JNDI name <xsl:value-of select="translate(@Name, ' ', '_')" />DS</xsl:message>
		  	<xsl:choose>
			<xsl:when test="starts-with(@ComponentType,'Apache ')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.derby.DerbySQLTranslator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'MySQL 5 ')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.mysql.MySQL5Translator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'MySQL JDBC')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.mysql.MySQLTranslator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'Oracle')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.oracle.OracleSQLTranslator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'PostgreSQL')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.postgresql.PostgreSQLTranslator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'SQL Server')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.sqlserver.SQLServerSQLTranslator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'DB2')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.sqlserver.DB2SQLTranslator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'H2')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.h2.H2Translator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'HSQLDQ')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.hsql.HSSQLTranslator</config-property>
		  	</xsl:when>
			<xsl:when test="starts-with(@ComponentType,'Sybase')">
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.sybase.SybaseSQLTranslator</config-property>
		  	</xsl:when>
		  	<xsl:otherwise>
		   <config-property name="ExtensionTranslationClassName">org.teiid.connector.jdbc.translator.Translator</config-property>
		  	</xsl:otherwise>
		   	</xsl:choose>
		   </xsl:if>
		   <xsl:if test="contains(@ComponentType,'XA')">
		   <config-property name="IsXA">true</config-property>
		   </xsl:if>
		   <xsl:if test="Properties/Property[@Name='ConnectorMaxConnections' or @Name='com.metamatrix.data.pool.max_connections']">
		   <max-pool-size><xsl:value-of select="Properties/Property[@Name='ConnectorMaxConnections' or @Name='com.metamatrix.data.pool.max_connections']/text()"/></max-pool-size>
		   </xsl:if>
		</no-tx-connection-factory>
		</xsl:for-each>
	</connection-factories>
	</xsl:if>
	</xsl:template>
</xsl:stylesheet>
