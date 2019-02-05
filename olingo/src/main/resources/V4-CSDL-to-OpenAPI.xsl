<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="3.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" xmlns:edm="http://docs.oasis-open.org/odata/ns/edm"
>
  <!--
    This style sheet transforms OData 4.0 CSDL XML documents into OpenAPI 2.0 or OpenAPI 3.0.0 JSON

    Latest version: https://github.com/oasis-tcs/odata-openapi/blob/master/tools/V4-CSDL-to-OpenAPI.xsl

    TODO:
    - delta: headers Prefer and Preference-Applied
    - operation descriptions for entity sets and singletons
    - custom headers and query options - https://issues.oasis-open.org/browse/ODATA-1099
    - response codes and descriptions - https://issues.oasis-open.org/browse/ODATA-884
    - inline definitions for Edm.* to make OpenAPI documents self-contained
    - securityDefinitions script parameter with default
    "securityDefinitions":{"basic_auth":{"type":"basic","description": "Basic
    Authentication"}}
    - complex or collection-valued function parameters need special treatment in /paths,
    use parameter aliases with alias option of type string
    - @Extends for entity container: include /paths from referenced container
    - both "clickable" and freestyle $expand, $select, $orderby - does not work yet, open issue for Swagger UI
    - system query options for actions/functions/imports depending on "Collection("
    - 200 response for PATCH if $odata-version!='2.0'
    - ETag for GET / If-Match for PATCH and DELETE depending on @Core.OptimisticConcurrency
    - reduce duplicated code in /paths production
    - external targeting for Capabilities: NonSortableProperties, KeyAsSegmentSupported
    - external targeting for Core: Immutable, Computed, Permission/Read
    - key property aliases
    - example values via Core.Example
  -->
  
  <!-- <xsl:mode streamable="yes"/> -->

  <xsl:output method="text" indent="yes" encoding="UTF-8" omit-xml-declaration="yes" />
  <xsl:strip-space elements="*" />


  <xsl:param name="scheme" select="'http'" />
  <xsl:param name="host" select="'localhost'" />
  <xsl:param name="basePath" select="'/service-root'" />

  <xsl:param name="info-title" select="null" />
  <xsl:param name="info-description" select="null" />
  <xsl:param name="info-version" select="null" />

  <xsl:param name="externalDocs-url" select="null" />
  <xsl:param name="externalDocs-description" select="null" />

  <xsl:param name="property-longDescription" select="true()" />

  <xsl:param name="x-tensions" select="null" />

  <xsl:param name="odata-version" select="'4.0'" />
  <xsl:param name="diagram" select="null" />
  <xsl:param name="references" select="null" />
  <xsl:param name="top-example" select="50" />

  <xsl:param name="odata-schema" select="'https://oasis-tcs.github.io/odata-openapi/examples/odata-definitions.json'" />
  <xsl:param name="openapi-formatoption" select="''" />
  <xsl:param name="openapi-version" select="'2.0'" />
  <xsl:param name="openapi-root" select="''" />

  <xsl:variable name="csdl-version" select="/edmx:Edmx/@Version" />
  <xsl:variable name="option-prefix">
    <xsl:choose>
      <xsl:when test="/edmx:Edmx/@Version='4.0'">
        <xsl:text>$</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="reuse-schemas">
    <xsl:choose>
      <xsl:when test="$openapi-version='2.0'">
        <xsl:text>#/definitions/</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>#/components/schemas/</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="reuse-parameters">
    <xsl:choose>
      <xsl:when test="$openapi-version='2.0'">
        <xsl:text>#/parameters/</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>#/components/parameters/</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="coreNamespace" select="'Org.OData.Core.V1'" />
  <xsl:variable name="coreAlias">
    <xsl:choose>
      <xsl:when test="//edmx:Include[@Namespace=$coreNamespace]/@Alias">
        <xsl:value-of select="//edmx:Include[@Namespace=$coreNamespace]/@Alias" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>Core</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="coreDescription" select="concat($coreNamespace,'.Description')" />
  <xsl:variable name="coreDescriptionAliased" select="concat($coreAlias,'.Description')" />
  <xsl:variable name="coreLongDescription" select="concat($coreNamespace,'.LongDescription')" />
  <xsl:variable name="coreLongDescriptionAliased" select="concat($coreAlias,'.LongDescription')" />

  <xsl:variable name="capabilitiesNamespace" select="'Org.OData.Capabilities.V1'" />
  <xsl:variable name="capabilitiesAlias" select="//edmx:Include[@Namespace=$capabilitiesNamespace]/@Alias" />

  <xsl:variable name="validationNamespace" select="'Org.OData.Validation.V1'" />
  <xsl:variable name="validationAlias" select="//edmx:Include[@Namespace=$validationNamespace]/@Alias" />

  <xsl:variable name="commonNamespace" select="'com.sap.vocabularies.Common.v1'" />
  <xsl:variable name="commonAlias" select="//edmx:Include[@Namespace=$commonNamespace]/@Alias" />
  <xsl:variable name="commonLabel" select="concat($commonNamespace,'.Label')" />
  <xsl:variable name="commonLabelAliased" select="concat($commonAlias,'.Label')" />
  <xsl:variable name="commonQuickInfo" select="concat($commonNamespace,'.QuickInfo')" />
  <xsl:variable name="commonQuickInfoAliased" select="concat($commonAlias,'.QuickInfo')" />

  <xsl:variable name="defaultResponse">
    <xsl:text>"default":{"$ref":"#/</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>components/</xsl:text>
    </xsl:if>
    <xsl:text>responses/error"}</xsl:text>
  </xsl:variable>

  <xsl:variable name="key-as-segment"
    select="//edm:EntityContainer/edm:Annotation[(@Term=concat($capabilitiesNamespace,'.KeyAsSegmentSupported') or @Term=concat($capabilitiesAlias,'.KeyAsSegmentSupported')) and not(@Qualifier)]" />

  <xsl:template name="capability">
    <xsl:param name="term" />
    <xsl:param name="property" select="false()" />
    <xsl:param name="target" select="." />
    <xsl:variable name="target-path" select="concat($target/../../@Namespace,'.',$target/../@Name,'/',$target/@Name)" />
    <xsl:variable name="target-path-aliased" select="concat($target/../../@Alias,'.',$target/../@Name,'/',$target/@Name)" />
    <xsl:variable name="anno"
      select="//edm:Annotations[(@Target=$target-path or @Target=$target-path-aliased)]/edm:Annotation[@Term=concat($capabilitiesNamespace,'.',$term) or @Term=concat($capabilitiesAlias,'.',$term)]
                                                                               |$target/edm:Annotation[@Term=concat($capabilitiesNamespace,'.',$term) or @Term=concat($capabilitiesAlias,'.',$term)]" />
    <xsl:choose>
      <xsl:when test="$property">
        <xsl:value-of
          select="$anno/edm:Record/edm:PropertyValue[@Property=$property]/@Bool
                 |$anno/edm:Record/edm:PropertyValue[@Property=$property]/edm:Bool" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$anno/@Bool|$anno/edm:Bool" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="capability-indexablebykey">
    <xsl:param name="term" select="'IndexableByKey'" />
    <xsl:param name="target" select="." />
    <xsl:variable name="target-path" select="concat($target/../../@Namespace,'.',$target/../@Name,'/',$target/@Name)" />
    <xsl:variable name="target-path-aliased" select="concat($target/../../@Alias,'.',$target/../@Name,'/',$target/@Name)" />
    <xsl:variable name="anno"
      select="//edm:Annotations[(@Target=$target-path or @Target=$target-path-aliased)]/edm:Annotation[(@Term=concat($capabilitiesNamespace,'.',$term) or @Term=concat($capabilitiesAlias,'.',$term))] 
                                                                               |$target/edm:Annotation[(@Term=concat($capabilitiesNamespace,'.',$term) or @Term=concat($capabilitiesAlias,'.',$term))]" />
    <xsl:choose>
      <xsl:when test="$anno/@Bool|$anno/edm:Bool">
        <xsl:value-of select="$anno/@Bool|$anno/edm:Bool" />
      </xsl:when>
      <xsl:when test="$anno">
        <xsl:text>true</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <!-- true would be the correct default -->
        <xsl:text>unspecified</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="annotation-string">
    <xsl:param name="node" />
    <xsl:param name="term" />
    <xsl:param name="termAliased" />
    <xsl:param name="qualifier" select="null" />
    <xsl:variable name="annotation"
      select="$node/edm:Annotation[(@Term=$term or @Term=$termAliased) and ((not($qualifier) and not(@Qualifier)) or $qualifier=@Qualifier)]" />
    <xsl:variable name="description" select="$annotation/@String|$annotation/edm:String" />
    <xsl:choose>
      <xsl:when test="$description">
        <xsl:call-template name="escape">
          <xsl:with-param name="string" select="$description" />
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:variable name="target">
          <xsl:call-template name="annotation-target">
            <xsl:with-param name="node" select="$node" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="targetAliased">
          <xsl:call-template name="annotation-target">
            <xsl:with-param name="node" select="$node" />
            <xsl:with-param name="qualifier" select="$node/ancestor::edm:Schema/@Alias" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="annotationExt"
          select="//edm:Annotations[(@Target=$target or @Target=$targetAliased) and not(@Qualifier)]/edm:Annotation[@Term=(@Term=$term or @Term=$termAliased) and ((not($qualifier) and not(@Qualifier)) or $qualifier=@Qualifier)]" />
        <xsl:call-template name="escape">
          <xsl:with-param name="string" select="$annotationExt/@String|$annotationExt/edm:String" />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="annotation-target">
    <xsl:param name="node" />
    <xsl:param name="qualifier" select="$node/ancestor::edm:Schema/@Namespace" />
    <xsl:choose>
      <xsl:when test="local-name($node)='Parameter' and $odata-version='2.0'">
        <xsl:value-of select="concat($qualifier,'.',$node/../../edm:EntityContainer/@Name,'/',$node/../@Name,'/',$node/@Name)" />
      </xsl:when>
      <xsl:when
        test="local-name($node)='Property' or local-name($node)='NavigationProperty'
              or local-name($node)='EntitySet' or local-name($node)='Singleton' 
              or local-name($node)='ActionImport' or local-name($node)='FunctionImport'"
      >
        <xsl:value-of select="concat($qualifier,'.',$node/../@Name,'/',$node/@Name)" />
      </xsl:when>
      <xsl:when test="local-name($node)='Parameter'">
        <xsl:value-of select="concat($qualifier,'.',$node/../@Name)" />
        <xsl:text>(</xsl:text>
        <xsl:for-each
          select="$node/../edm:Parameter[local-name($node/..)='Function' or ($node/../@IsBound='true' and position()=1)]"
        >
          <xsl:if test="position()>1">
            <xsl:text>,</xsl:text>
          </xsl:if>
          <xsl:value-of select="@Type" />
        </xsl:for-each>
        <xsl:text>)</xsl:text>
        <xsl:value-of select="concat('/',$node/@Name)" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="concat($qualifier,'.',$node/@Name)" />
        <xsl:if test="local-name($node)='Action' or local-name($node)='Function'">
          <xsl:text>(</xsl:text>
          <xsl:for-each select="$node/edm:Parameter[local-name($node)='Function' or ($node/@IsBound='true' and position()=1)]">
            <xsl:if test="position()>1">
              <xsl:text>,</xsl:text>
            </xsl:if>
            <xsl:value-of select="@Type" />
          </xsl:for-each>
          <xsl:text>)</xsl:text>
        </xsl:if>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="Core.Description">
    <xsl:param name="node" />
    <xsl:param name="qualifier" select="null" />
    <xsl:call-template name="annotation-string">
      <xsl:with-param name="node" select="$node" />
      <xsl:with-param name="term" select="$coreDescription" />
      <xsl:with-param name="termAliased" select="$coreDescriptionAliased" />
      <xsl:with-param name="qualifier" select="$qualifier" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="Core.LongDescription">
    <xsl:param name="node" />
    <xsl:param name="qualifier" select="null" />
    <xsl:call-template name="annotation-string">
      <xsl:with-param name="node" select="$node" />
      <xsl:with-param name="term" select="$coreLongDescription" />
      <xsl:with-param name="termAliased" select="$coreLongDescriptionAliased" />
      <xsl:with-param name="qualifier" select="$qualifier" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="Common.Label">
    <xsl:param name="node" />
    <xsl:call-template name="annotation-string">
      <xsl:with-param name="node" select="$node" />
      <xsl:with-param name="term" select="$commonLabel" />
      <xsl:with-param name="termAliased" select="$commonLabelAliased" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="Common.QuickInfo">
    <xsl:param name="node" />
    <xsl:call-template name="annotation-string">
      <xsl:with-param name="node" select="$node" />
      <xsl:with-param name="term" select="$commonQuickInfo" />
      <xsl:with-param name="termAliased" select="$commonQuickInfoAliased" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template match="edmx:Edmx">
    <xsl:text>{</xsl:text>
    <xsl:choose>
      <xsl:when test="$openapi-version='2.0'">
        <xsl:text>"swagger":"2.0"</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>"openapi":"3.0.0"</xsl:text>
      </xsl:otherwise>
    </xsl:choose>

    <xsl:text>,"info":{"title":"</xsl:text>
    <xsl:variable name="schemaDescription">
      <xsl:call-template name="Core.Description">
        <xsl:with-param name="node" select="//edm:Schema" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="containerDescription">
      <xsl:call-template name="Core.Description">
        <xsl:with-param name="node" select="//edm:EntityContainer" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test="$info-title">
        <xsl:value-of select="$info-title" />
      </xsl:when>
      <xsl:when test="$schemaDescription!=''">
        <xsl:value-of select="$schemaDescription" />
      </xsl:when>
      <xsl:when test="$containerDescription!=''">
        <xsl:value-of select="$containerDescription" />
      </xsl:when>
      <xsl:when test="//edm:EntityContainer">
        <xsl:text>OData Service for namespace </xsl:text>
        <xsl:value-of select="//edm:EntityContainer/../@Namespace" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>OData CSDL Document for namespace </xsl:text>
        <xsl:value-of select="//edm:Schema/@Namespace" />
      </xsl:otherwise>
    </xsl:choose>

    <xsl:text>","version":"</xsl:text>
    <xsl:choose>
      <xsl:when test="$info-version">
        <xsl:value-of select="$info-version" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="annotation-string">
          <xsl:with-param name="node" select="//edm:Schema" />
          <xsl:with-param name="term" select="concat($coreNamespace,'.SchemaVersion')" />
          <xsl:with-param name="termAliased" select="concat($coreAlias,'.SchemaVersion')" />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>

    <xsl:text>","description":"</xsl:text>
    <xsl:variable name="schemaLongDescription">
      <xsl:call-template name="Core.LongDescription">
        <xsl:with-param name="node" select="//edm:Schema" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="containerLongDescription">
      <xsl:call-template name="Core.LongDescription">
        <xsl:with-param name="node" select="//edm:EntityContainer" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test="$info-description">
        <xsl:value-of select="$info-description" />
      </xsl:when>
      <xsl:when test="$schemaLongDescription!=''">
        <xsl:value-of select="$schemaLongDescription" />
      </xsl:when>
      <xsl:when test="$containerLongDescription!=''">
        <xsl:value-of select="$containerLongDescription" />
      </xsl:when>
      <xsl:when test="//edm:EntityContainer">
        <xsl:text>This OData service is located at [</xsl:text>
        <xsl:value-of select="$scheme" />
        <xsl:text>://</xsl:text>
        <xsl:value-of select="$host" />
        <xsl:value-of select="$basePath" />
        <xsl:text>/](</xsl:text>
        <xsl:value-of select="$scheme" />
        <xsl:text>://</xsl:text>
        <xsl:value-of select="$host" />
        <xsl:call-template name="replace-all">
          <xsl:with-param name="string">
            <xsl:call-template name="replace-all">
              <xsl:with-param name="string" select="$basePath" />
              <xsl:with-param name="old" select="'('" />
              <xsl:with-param name="new" select="'%28'" />
            </xsl:call-template>
          </xsl:with-param>
          <xsl:with-param name="old" select="')'" />
          <xsl:with-param name="new" select="'%29'" />
        </xsl:call-template>
        <xsl:text>/)</xsl:text>
      </xsl:when>
    </xsl:choose>
    <xsl:if test="$diagram">
      <xsl:apply-templates select="//edm:EntityType|//edm:ComplexType" mode="description" />
    </xsl:if>
    <xsl:if test="$references">
      <xsl:apply-templates select="//edmx:Include" mode="description" />
    </xsl:if>
    <xsl:text>"}</xsl:text>

    <xsl:if test="$externalDocs-url">
      <xsl:text>,"externalDocs":{</xsl:text>
      <xsl:if test="$externalDocs-description">
        <xsl:text>"description":"</xsl:text>
        <xsl:value-of select="$externalDocs-description" />
        <xsl:text>",</xsl:text>
      </xsl:if>
      <xsl:text>"url":"</xsl:text>
      <xsl:value-of select="$externalDocs-url" />
      <xsl:text>"}</xsl:text>
    </xsl:if>

    <xsl:if test="$x-tensions">
      <xsl:text>,</xsl:text>
      <xsl:value-of select="$x-tensions" />
    </xsl:if>

    <xsl:if test="//edm:EntityContainer">
      <xsl:choose>
        <xsl:when test="$openapi-version='2.0'">
          <xsl:text>,"schemes":["</xsl:text>
          <xsl:value-of select="$scheme" />
          <xsl:text>"],"host":"</xsl:text>
          <xsl:value-of select="$host" />
          <xsl:text>","basePath":"</xsl:text>
          <xsl:value-of select="$basePath" />
          <xsl:text>"</xsl:text>

          <!-- TODO: Capabilities.SupportedFormats -->
          <xsl:text>,"consumes":["application/json"]</xsl:text>
          <xsl:text>,"produces":["application/json"]</xsl:text>
        </xsl:when>
        <xsl:otherwise>
          <xsl:text>,"servers":[{"url":"</xsl:text>
          <xsl:value-of select="$scheme" />
          <xsl:text>://</xsl:text>
          <xsl:value-of select="$host" />
          <xsl:value-of select="$basePath" />
          <xsl:text>"}]</xsl:text>
        </xsl:otherwise>
      </xsl:choose>

    </xsl:if>

    <xsl:apply-templates select="//edm:EntitySet|//edm:Singleton" mode="tags" />

    <!-- paths is required, so we need it also for documents that do not define an entity container -->
    <xsl:text>,"paths":{</xsl:text>
    <xsl:apply-templates select="//edm:EntityContainer" mode="paths" />
    <xsl:text>}</xsl:text>

    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>,"components":{</xsl:text>
    </xsl:if>

    <xsl:apply-templates
      select="//edm:EntityType|//edm:ComplexType|//edm:TypeDefinition|//edm:EnumType|//edm:EntityContainer" mode="hash"
    >
      <xsl:with-param name="name">
        <xsl:choose>
          <xsl:when test="$openapi-version='2.0'">
            <xsl:text>definitions</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>schemas</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:with-param>
      <xsl:with-param name="after" select="$openapi-version='2.0'" />
    </xsl:apply-templates>

    <xsl:if test="//edm:EntityContainer">
      <xsl:text>,"parameters":{</xsl:text>
      <xsl:text>"top":{"name":"</xsl:text>
      <xsl:value-of select="$option-prefix" />
      <xsl:text>top","in":"query","description":"Show only the first n items</xsl:text>
      <xsl:text>, see [OData Paging - Top](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptiontop)",</xsl:text>
      <xsl:call-template name="parameter-type">
        <xsl:with-param name="type" select="'integer'" />
        <xsl:with-param name="plus" select="',&quot;minimum&quot;:0'" />
      </xsl:call-template>
      <xsl:if test="number($top-example) and $openapi-version!='2.0'">
        <xsl:text>,"example":</xsl:text>
        <xsl:value-of select="$top-example" />
      </xsl:if>
      <xsl:text>},</xsl:text>
      <xsl:text>"skip":{"name":"</xsl:text>
      <xsl:value-of select="$option-prefix" />
      <xsl:text>skip","in":"query","description":"Skip the first n items</xsl:text>
      <xsl:text>, see [OData Paging - Skip](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionskip)",</xsl:text>
      <xsl:call-template name="parameter-type">
        <xsl:with-param name="type" select="'integer'" />
        <xsl:with-param name="plus" select="',&quot;minimum&quot;:0'" />
      </xsl:call-template>
      <xsl:text>},</xsl:text>
      <xsl:choose>
        <xsl:when test="substring($odata-version,1,3)='4.0'">
          <xsl:text>"count":{"name":"</xsl:text>
          <xsl:value-of select="$option-prefix" />
          <xsl:text>count","in":"query","description":"Include count of items</xsl:text>
          <xsl:text>, see [OData Count](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptioncount)",</xsl:text>
          <xsl:call-template name="parameter-type">
            <xsl:with-param name="type" select="'boolean'" />
          </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
          <xsl:text>"count":{"name": "$inlinecount","in":"query","description":"Include count of items</xsl:text>
          <xsl:text>, see [OData Count](http://www.odata.org/documentation/odata-version-2-0/uri-conventions/#InlinecountSystemQueryOption)",</xsl:text>
          <xsl:call-template name="parameter-type">
            <xsl:with-param name="type" select="'string'" />
            <xsl:with-param name="plus">
              <xsl:text>,"enum":["allpages","none"]</xsl:text>
            </xsl:with-param>
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
      <xsl:text>}</xsl:text>
      <xsl:choose>
        <xsl:when test="substring($odata-version,1,3)='4.0'">
          <xsl:text>,"search":{"name":"</xsl:text>
          <xsl:value-of select="$option-prefix" />
          <xsl:text>search","in":"query","description":"Search items by search phrases</xsl:text>
          <xsl:text>, see [OData Searching](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionsearch)",</xsl:text>
          <xsl:call-template name="parameter-type">
            <xsl:with-param name="type" select="'string'" />
          </xsl:call-template>
          <xsl:text>}</xsl:text>
        </xsl:when>
        <xsl:when
          test="//edm:Annotation[@Term=concat($capabilitiesNamespace,'.SearchRestrictions') or @Term=concat($capabilitiesAlias,'.SearchRestrictions')]/edm:Record/edm:PropertyValue[@Property='Searchable' and @Bool='true']"
        >
          <xsl:text>,"search":{"name":"search","in":"query","description":"Search items by search phrases</xsl:text>
          <xsl:text>, see [OData Searching](https://wiki.scn.sap.com/wiki/display/EmTech/SAP+Annotations+for+OData+Version+2.0#SAPAnnotationsforODataVersion2.0-Query_Option_searchQueryOptionsearch)",</xsl:text>
          <xsl:call-template name="parameter-type">
            <xsl:with-param name="type" select="'string'" />
          </xsl:call-template>
          <xsl:text>}</xsl:text>
        </xsl:when>
      </xsl:choose>
      <xsl:text>}</xsl:text>

      <xsl:text>,"responses":{"error":{"description":"Error",</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>"content":{"application/json":{</xsl:text>
      </xsl:if>
      <xsl:text>"schema":{"$ref":"</xsl:text>
      <xsl:value-of select="$reuse-schemas" />
      <xsl:text>odata.error"}</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>}}</xsl:text>
      </xsl:if>
      <xsl:text>}}</xsl:text>
    </xsl:if>

    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>}</xsl:text>
    </xsl:if>

    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template name="parameter-type">
    <xsl:param name="type" />
    <xsl:param name="plus" select="null" />

    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>"schema":{</xsl:text>
    </xsl:if>
    <xsl:text>"type":"</xsl:text>
    <xsl:value-of select="$type" />
    <xsl:text>"</xsl:text>

    <xsl:if test="$plus">
      <xsl:value-of select="$plus" />
    </xsl:if>

    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>}</xsl:text>
    </xsl:if>
  </xsl:template>

  <!-- definitions for standard error response - only needed if there's an entity container -->
  <xsl:template match="edm:EntityContainer" mode="hashpair">
    <xsl:if test="//@Type[.='Edm.GeographyPoint' or .='Edm.GeometryPoint']">
      <xsl:text>"geoPoint":{"type":"object","properties":{"type":{"type":"string","enum":["Point"],"default":"Point"},"coordinates":{"$ref":"</xsl:text>
      <xsl:value-of select="$reuse-schemas" />
      <xsl:text>geoPosition"}},"required":["type","coordinates"]},</xsl:text>
    </xsl:if>
    <xsl:if test="//@Type[starts-with(.,'Edm.Geo')]">
      <xsl:text>"geoPosition":{"type":"array","items":{"type":"number"},"minItems":2},</xsl:text>
    </xsl:if>
    <xsl:text>"odata.error":{"type":"object","required":["error"],"properties":{"error":{"$ref":"</xsl:text>
    <xsl:value-of select="$reuse-schemas" />
    <xsl:text>odata.error.main"}}}</xsl:text>
    <xsl:text>,"odata.error.main":{"type":"object","required":["code","message"],"properties":{"code":{"type":"string"},"message":</xsl:text>
    <xsl:choose>
      <xsl:when test="substring($odata-version,1,3)='4.0'">
        <xsl:text>{"type":"string"},"target":{"type":"string"},"details":</xsl:text>
        <xsl:text>{"type":"array","items":{"$ref":"</xsl:text>
        <xsl:value-of select="$reuse-schemas" />
        <xsl:text>odata.error.detail"}}</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>{"$ref":"</xsl:text>
        <xsl:value-of select="$reuse-schemas" />
        <xsl:text>odata.error.message"}</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>,"innererror":{"type":"object","description":"The structure of this object is service-specific"}}}</xsl:text>
    <xsl:choose>
      <xsl:when test="substring($odata-version,1,3)='4.0'">
        <xsl:text>,"odata.error.detail":{"type":"object","required":["code","message"],"properties":{"code":{"type":"string"},"message":{"type":"string"},"target":{"type":"string"}}}</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>,"odata.error.message":{"type":"object","required":["lang","value"],"properties":{"lang":{"type":"string"},"value":{"type":"string"}}}</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="edm:EntityType|edm:ComplexType" mode="description">
    <xsl:if test="position() = 1">
      <xsl:text>\n\n## Entity Data Model\n![ER Diagram](https://yuml.me/diagram/class/</xsl:text>
    </xsl:if>
    <xsl:if test="position() > 1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:apply-templates select="@BaseType" mode="description" />
    <xsl:text>[</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:if test="local-name()='EntityType'">
      <xsl:text>{bg:orange}</xsl:text>
    </xsl:if>
    <xsl:text>]</xsl:text>
    <xsl:apply-templates select="edm:NavigationProperty|edm:Property" mode="description" />
    <xsl:if test="position() = last()">
      <xsl:text>)</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template match="@BaseType" mode="description">
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="." />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="type">
      <xsl:call-template name="substring-after-last">
        <xsl:with-param name="input" select="." />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:text>[</xsl:text>
    <xsl:choose>
      <xsl:when test="$qualifier=//edm:Schema/@Namespace or $qualifier=//edm:Schema/@Alias">
        <xsl:value-of select="$type" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="normalizedQualifiedName">
          <xsl:with-param name="qualifiedName" select="." />
        </xsl:call-template>
        <xsl:text>{bg:whitesmoke}</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>]^</xsl:text>
  </xsl:template>

  <xsl:template match="edm:NavigationProperty|edm:Property" mode="description">
    <xsl:variable name="singleType">
      <xsl:choose>
        <xsl:when test="starts-with(@Type,'Collection(')">
          <xsl:value-of select="substring-before(substring-after(@Type,'('),')')" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="@Type" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="collection" select="starts-with(@Type,'Collection(')" />
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="$singleType" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="type">
      <xsl:call-template name="substring-after-last">
        <xsl:with-param name="input" select="$singleType" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="nullable">
      <xsl:call-template name="nullableFacetValue">
        <xsl:with-param name="type" select="@Type" />
        <xsl:with-param name="nullableFacet" select="@Nullable" />
      </xsl:call-template>
    </xsl:variable>
    <!--
      TODO: evaluate Partner to just have one arrow
      [FeaturedProduct]<0..1-0..1>[Advertisement]
    -->
    <xsl:if test="$qualifier!='Edm' or local-name='NavigationProperty'">
      <xsl:text>,[</xsl:text>
      <xsl:value-of select="../@Name" />
      <xsl:text>]</xsl:text>
      <xsl:if test="@ContainsTarget='true'">
        <xsl:text>++</xsl:text>
      </xsl:if>
      <xsl:text>-</xsl:text>
      <xsl:choose>
        <xsl:when test="$collection">
          <xsl:text>*</xsl:text>
        </xsl:when>
        <xsl:when test="$nullable='true'">
          <xsl:text>0..1</xsl:text>
        </xsl:when>
      </xsl:choose>
      <xsl:text>>[</xsl:text>
      <xsl:choose>
        <xsl:when test="$qualifier=//edm:Schema/@Namespace or $qualifier=//edm:Schema/@Alias">
          <xsl:value-of select="$type" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="normalizedQualifiedName">
            <xsl:with-param name="qualifiedName" select="$singleType" />
          </xsl:call-template>
          <xsl:text>{bg:whitesmoke}</xsl:text>
        </xsl:otherwise>
      </xsl:choose>
      <xsl:text>]</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edmx:Include" mode="description">
    <xsl:if test="position() = 1">
      <xsl:text>\n\n## References</xsl:text>
    </xsl:if>
    <xsl:text>\n- [</xsl:text>
    <xsl:value-of select="@Namespace" />
    <xsl:text>](</xsl:text>
    <xsl:choose>
      <xsl:when test="substring(@Namespace,1,10)='Org.OData.'">
        <xsl:text>https://github.com/oasis-tcs/odata-vocabularies/blob/master/vocabularies/</xsl:text>
        <xsl:value-of select="@Namespace" />
        <xsl:text>.md</xsl:text>
      </xsl:when>
      <xsl:when test="substring(@Namespace,1,21)='com.sap.vocabularies.'">
        <xsl:text>https://wiki.scn.sap.com/wiki/display/EmTech/OData+4.0+Vocabularies+-+SAP+</xsl:text>
        <xsl:value-of select="substring(@Namespace,22,string-length(@Namespace)-24)" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>?url=</xsl:text>
        <xsl:call-template name="replace-all">
          <xsl:with-param name="string">
            <xsl:call-template name="json-url">
              <xsl:with-param name="url" select="../@Uri" />
              <xsl:with-param name="root" select="$openapi-root" />
            </xsl:call-template>
          </xsl:with-param>
          <xsl:with-param name="old" select="')'" />
          <xsl:with-param name="new" select="'%29'" />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>)</xsl:text>
  </xsl:template>

  <xsl:template match="edm:EnumType" mode="hashpair">
    <xsl:text>"</xsl:text>
    <xsl:value-of select="../@Namespace" />
    <xsl:text>.</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>":{"type":"string",</xsl:text>
    <xsl:text>"enum":[</xsl:text>
    <xsl:apply-templates select="edm:Member" mode="enum" />
    <xsl:text>]</xsl:text>
    <xsl:call-template name="title-description">
      <xsl:with-param name="fallback-title" select="@Name" />
    </xsl:call-template>
    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:Member" mode="enum">
    <xsl:if test="position() > 1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>"</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>"</xsl:text>
  </xsl:template>

  <xsl:template match="edm:TypeDefinition" mode="hashpair">
    <xsl:text>"</xsl:text>
    <xsl:value-of select="../@Namespace" />
    <xsl:text>.</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>":{</xsl:text>
    <xsl:call-template name="type">
      <xsl:with-param name="type" select="@UnderlyingType" />
      <xsl:with-param name="nullableFacet" select="'false'" />
    </xsl:call-template>
    <xsl:call-template name="title-description">
      <xsl:with-param name="fallback-title" select="@Name" />
    </xsl:call-template>
    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:EntityType|edm:ComplexType" mode="hashpair">
    <xsl:variable name="typeName" select="concat(../@Namespace,'.',@Name)" />
    <!-- TODO: also external annotations - testcase - run-time -->
    <xsl:variable name="computed"
      select="edm:Property[edm:Annotation[@Term='Org.OData.Core.V1.Computed' or @Term=concat($coreAlias,'.Computed')]]/@Name" />
    <xsl:variable name="immutable"
      select="edm:Property[edm:Annotation[@Term='Org.OData.Core.V1.Immutable' or @Term=concat($coreAlias,'.Immutable')]]/@Name" />
    <!-- TODO: make expression catch all alias variations in @Target, @Term, and @EnumMember -->
    <xsl:variable name="read-only"
      select="edm:Property[edm:Annotation[@Term='Org.OData.Core.V1.Permissions' or @Term=concat($coreAlias,'.Permissions')]/edm:EnumMember='Org.OData.Core.V1.Permission/Read']/@Name" />
    <!-- TODO: make expression catch all alias variations in @Target, @Term, and @EnumMember -->
    <xsl:variable name="mandatory"
      select="//edm:Annotations[edm:Annotation[@Term=concat($commonAlias,'.FieldControl') and @EnumMember=concat($commonAlias,'.FieldControlType/Mandatory')] and $typeName=substring-before(@Target,'/')]/@Target" />

    <!-- full structure -->
    <xsl:text>"</xsl:text>
    <xsl:value-of select="$typeName" />
    <xsl:text>":{</xsl:text>

    <xsl:if test="@BaseType">
      <xsl:text>"allOf":[{</xsl:text>
      <xsl:call-template name="schema-ref">
        <xsl:with-param name="qualifiedName" select="@BaseType" />
      </xsl:call-template>
      <xsl:text>},{</xsl:text>
    </xsl:if>

    <xsl:text>"type":"object"</xsl:text>
    <xsl:apply-templates select="edm:Property|edm:NavigationProperty" mode="hash">
      <xsl:with-param name="name" select="'properties'" />
    </xsl:apply-templates>

    <xsl:if test="@BaseType">
      <xsl:text>}]</xsl:text>
    </xsl:if>

    <xsl:call-template name="title-description">
      <xsl:with-param name="fallback-title" select="@Name" />
    </xsl:call-template>
    <xsl:text>}</xsl:text>

    <!-- create structure -->
    <xsl:text>,"</xsl:text>
    <xsl:value-of select="$typeName" />
    <xsl:text>-create":{</xsl:text>

    <xsl:if test="@BaseType">
      <xsl:text>"allOf":[{</xsl:text>
      <xsl:call-template name="schema-ref">
        <xsl:with-param name="qualifiedName" select="@BaseType" />
        <xsl:with-param name="suffix" select="'-create'" />
      </xsl:call-template>
      <xsl:text>},{</xsl:text>
    </xsl:if>

    <xsl:text>"type":"object"</xsl:text>
    <!-- everything except computed and read-only properties -->
    <xsl:apply-templates select="edm:Property[not(@Name=$computed or @Name=$read-only)]|edm:NavigationProperty"
      mode="hash"
    >
      <xsl:with-param name="name" select="'properties'" />
      <xsl:with-param name="suffix" select="'-create'" />
    </xsl:apply-templates>
    <!-- non-computed key properties are required, as are properties marked with Common.FieldControl=Mandatory -->
    <xsl:apply-templates
      select="edm:Property[(@Name=../edm:Key/edm:PropertyRef/@Name and not(@Name=$computed or @Name=$read-only)) or concat($typeName,'/',@Name)=$mandatory]"
      mode="required" />

    <xsl:if test="@BaseType">
      <xsl:text>}]</xsl:text>
    </xsl:if>

    <xsl:call-template name="title-description">
      <xsl:with-param name="fallback-title" select="@Name" />
      <xsl:with-param name="suffix" select="' (for create)'" />
    </xsl:call-template>
    <xsl:text>}</xsl:text>

    <!-- update structure -->
    <xsl:text>,"</xsl:text>
    <xsl:value-of select="$typeName" />
    <xsl:text>-update":{</xsl:text>

    <xsl:if test="@BaseType">
      <xsl:text>"allOf":[{</xsl:text>
      <xsl:call-template name="schema-ref">
        <xsl:with-param name="qualifiedName" select="@BaseType" />
        <xsl:with-param name="suffix" select="'-update'" />
      </xsl:call-template>
      <xsl:text>},{</xsl:text>
    </xsl:if>

    <xsl:text>"type":"object"</xsl:text>
    <!-- only updatable non-key properties -->
    <xsl:apply-templates
      select="edm:Property[not(@Name=$immutable or @Name=$computed or @Name=$read-only or @Name=../edm:Key/edm:PropertyRef/@Name)]"
      mode="hash"
    >
      <xsl:with-param name="name" select="'properties'" />
      <xsl:with-param name="suffix" select="'-update'" />
    </xsl:apply-templates>

    <xsl:if test="@BaseType">
      <xsl:text>}]</xsl:text>
    </xsl:if>

    <xsl:call-template name="title-description">
      <xsl:with-param name="fallback-title" select="@Name" />
      <xsl:with-param name="suffix" select="' (for update)'" />
    </xsl:call-template>
    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:Property" mode="required">
    <xsl:if test="position() = 1">
      <xsl:text>,"required":[</xsl:text>
    </xsl:if>
    <xsl:if test="position()>1">
      <xsl:text>,</xsl:text>
    </xsl:if>

    <xsl:text>"</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>"</xsl:text>

    <xsl:if test="position() = last()">
      <xsl:text>]</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edm:Property|edm:NavigationProperty" mode="hashvalue">
    <xsl:param name="suffix" select="null" />
    <xsl:call-template name="type">
      <xsl:with-param name="type" select="@Type" />
      <xsl:with-param name="nullableFacet" select="@Nullable" />
      <xsl:with-param name="suffix" select="$suffix" />
    </xsl:call-template>
    <xsl:choose>
      <xsl:when test="local-name()='Property'">
        <xsl:apply-templates select="*[local-name()!='Annotation']" mode="list2" />
      </xsl:when>
      <xsl:otherwise>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:call-template name="title-description" />
  </xsl:template>

  <xsl:template name="nullableFacetValue">
    <xsl:param name="type" />
    <xsl:param name="nullableFacet" />
    <xsl:choose>
      <xsl:when test="$nullableFacet">
        <xsl:value-of select="$nullableFacet" />
      </xsl:when>
      <xsl:when test="starts-with($type,'Collection(')">
        <xsl:value-of select="'false'" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="'true'" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="type">
    <xsl:param name="type" />
    <xsl:param name="nullableFacet" />
    <xsl:param name="target" select="." />
    <xsl:param name="inParameter" select="false()" />
    <xsl:param name="inResponse" select="false()" />
    <xsl:param name="suffix" select="null" />
    <xsl:variable name="noArray" select="true()" />
    <xsl:variable name="nullable">
      <xsl:call-template name="nullableFacetValue">
        <xsl:with-param name="type" select="$type" />
        <xsl:with-param name="nullableFacet" select="$nullableFacet" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="collection" select="starts-with($type,'Collection(')" />
    <xsl:variable name="singleType">
      <xsl:choose>
        <xsl:when test="$collection">
          <xsl:value-of select="substring-before(substring-after($type,'('),')')" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$type" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="$singleType" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="simpleName">
      <xsl:call-template name="substring-after-last">
        <xsl:with-param name="input" select="$singleType" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="$collection">
      <xsl:if test="$odata-version='2.0'">
        <xsl:if test="$inResponse">
          <xsl:text>"title":"Collection of </xsl:text>
          <xsl:value-of select="$simpleName" />
          <xsl:text>",</xsl:text>
        </xsl:if>
        <xsl:text>"type":"object","properties":{"results":{</xsl:text>
      </xsl:if>
      <xsl:text>"type":"array","items":{</xsl:text>
    </xsl:if>
    <xsl:choose>
      <xsl:when test="$singleType='Edm.String'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:apply-templates select="$target/@MaxLength" />
        <xsl:call-template name="Validation.AllowedValues">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:choose>
          <xsl:when test="$inParameter and $odata-version='2.0'">
            <xsl:text>,"pattern":"^'[^']*(''[^']*)*'$"</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:call-template name="Validation.Pattern">
              <xsl:with-param name="target" select="$target" />
            </xsl:call-template>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:if test="not($inParameter) and not($nullable='false') and $openapi-version='2.0'">
          <xsl:text>,"example":"string"</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Stream'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:text>,"format":"base64url"</xsl:text>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Binary'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:choose>
          <xsl:when test="$inParameter and $odata-version='2.0'">
            <xsl:text>,"pattern":"^X'([0-9a-fA-F][0-9a-fA-F])*'$"</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>,"format":"base64url"</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:apply-templates select="$target/@MaxLength" />
      </xsl:when>
      <xsl:when test="$singleType='Edm.Boolean'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'boolean'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Decimal'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type">
            <xsl:choose>
              <xsl:when test="$odata-version='2.0'">
                <xsl:value-of select="'string'" />
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="'number,string'" />
              </xsl:otherwise>
            </xsl:choose>
          </xsl:with-param>
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:choose>
          <xsl:when test="$inParameter and $odata-version='2.0'">
            <xsl:text>,"pattern":"^[-]?[0-9]+(\\.[0-9]+)?[mM]$"</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>,"format":"decimal"</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:choose>
          <xsl:when test="not($target/@Scale) or $target/@Scale='0'">
            <xsl:text>,"multipleOf":1</xsl:text>
          </xsl:when>
          <!-- limit the multiple of as several conversion tools to yaml won't properly convert small numbers -->
          <xsl:when test="number($target/@Scale)=$target/@Scale and not(number($target/@Scale) > 323)">
            <xsl:text>,"multipleOf":1.0e-</xsl:text>
            <xsl:value-of select="$target/@Scale" />
          </xsl:when>
        </xsl:choose>
        <xsl:variable name="scale">
          <xsl:choose>
            <xsl:when test="number($target/@Scale)=$target/@Scale">
              <xsl:value-of select="$target/@Scale" />
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="0" />
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:variable name="limit">
          <xsl:choose>
            <xsl:when test="$target/@Precision > $scale">
              <xsl:call-template name="repeat">
                <xsl:with-param name="string" select="'9'" />
                <xsl:with-param name="count" select="$target/@Precision - $scale" />
              </xsl:call-template>
            </xsl:when>
            <xsl:when test="$target/@Precision = $scale">
              <xsl:text>0</xsl:text>
            </xsl:when>
          </xsl:choose>
          <xsl:if test="$scale > 0">
            <xsl:text>.</xsl:text>
            <xsl:call-template name="repeat">
              <xsl:with-param name="string" select="'9'" />
              <xsl:with-param name="count" select="$scale" />
            </xsl:call-template>
          </xsl:if>
        </xsl:variable>
        <xsl:variable name="minimum">
          <xsl:call-template name="Validation.Minimum">
            <xsl:with-param name="target" select="$target" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="maximum">
          <xsl:call-template name="Validation.Maximum">
            <xsl:with-param name="target" select="$target" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:choose>
          <xsl:when test="$minimum!=''">
            <xsl:value-of select="$minimum" />
          </xsl:when>
          <xsl:when test="$target/@Precision &lt; 16">
            <xsl:text>,"minimum":-</xsl:text>
            <xsl:value-of select="$limit" />
          </xsl:when>
        </xsl:choose>
        <xsl:choose>
          <xsl:when test="$maximum!=''">
            <xsl:value-of select="$maximum" />
          </xsl:when>
          <xsl:when test="$target/@Precision &lt; 16">
            <xsl:text>,"maximum":</xsl:text>
            <xsl:value-of select="$limit" />
          </xsl:when>
        </xsl:choose>
        <xsl:if test="not($inParameter and $openapi-version='2.0')">
          <xsl:text>,"example":</xsl:text>
          <xsl:if test="$odata-version='2.0'">
            <xsl:text>"</xsl:text>
          </xsl:if>
          <xsl:text>0</xsl:text>
          <xsl:if test="$odata-version='2.0'">
            <xsl:text>"</xsl:text>
          </xsl:if>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Byte'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'integer'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:text>,"format":"uint8"</xsl:text>
        <xsl:call-template name="Validation.Minimum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:call-template name="Validation.Maximum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="$singleType='Edm.SByte'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'integer'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:text>,"format":"int8"</xsl:text>
        <xsl:call-template name="Validation.Minimum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:call-template name="Validation.Maximum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Int16'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'integer'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:text>,"format":"int16"</xsl:text>
        <xsl:call-template name="Validation.Minimum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:call-template name="Validation.Maximum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Int32'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'integer'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:text>,"format":"int32"</xsl:text>
        <xsl:call-template name="Validation.Minimum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:call-template name="Validation.Maximum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Int64'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type">
            <xsl:choose>
              <xsl:when test="$odata-version='2.0'">
                <xsl:value-of select="'string'" />
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="'integer,string'" />
              </xsl:otherwise>
            </xsl:choose>
          </xsl:with-param>
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:choose>
          <xsl:when test="$inParameter and $odata-version='2.0'">
            <xsl:text>,"pattern":"^[-]?[0-9]+[lL]$"</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>,"format":"int64"</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
        <!-- TODO: make example depend on min-max -->
        <xsl:if test="not($inParameter and $openapi-version='2.0')">
          <xsl:text>,"example":"42"</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Date'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:choose>
          <xsl:when test="$inParameter and $odata-version='2.0'">
            <xsl:text>,"pattern":"^datetime'[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00'$"</xsl:text>
          </xsl:when>
          <xsl:when test="$odata-version='2.0'">
            <xsl:text>,"example":"/Date(1492041600000)/"</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>,"format":"date"</xsl:text>
            <xsl:if test="not($inParameter and $openapi-version='2.0')">
              <xsl:text>,"example":"2017-04-13"</xsl:text>
            </xsl:if>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Double'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'number,string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:text>,"format":"double"</xsl:text>
        <xsl:call-template name="Validation.Minimum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:call-template name="Validation.Maximum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:if test="not($inParameter and $openapi-version='2.0')">
          <xsl:text>,"example":3.14</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Single'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'number,string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:text>,"format":"float"</xsl:text>
        <xsl:call-template name="Validation.Minimum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:call-template name="Validation.Maximum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:if test="not($inParameter and $openapi-version='2.0')">
          <xsl:text>,"example":3.14</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Guid'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:choose>
          <xsl:when test="$inParameter and $odata-version='2.0'">
            <xsl:text>,"pattern":"^guid'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'$"</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>,"format":"uuid"</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:if test="not($inParameter)">
          <xsl:text>,"example":"01234567-89ab-cdef-0123-456789abcdef"</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$singleType='Edm.DateTimeOffset'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:choose>
          <xsl:when test="$inParameter and $odata-version='2.0'">
            <xsl:text>,"pattern":"^datetime'[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9](\\.[0-9]+)?)?'$"</xsl:text>
          </xsl:when>
          <xsl:when test="$odata-version='2.0'">
            <xsl:text>,"example":"/Date(1492098664000)/"</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>,"format":"date-time"</xsl:text>
            <xsl:if test="not($inParameter)">
              <xsl:text>,"example":"2017-04-13T15:51:04Z"</xsl:text>
            </xsl:if>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:when test="$singleType='Edm.TimeOfDay'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:choose>
          <xsl:when test="$inParameter and $odata-version='2.0'">
            <xsl:text>,"pattern":"^time'PT(([01]?[0-9]|2[0-3])H)?([0-5]?[0-9]M)?([0-5]?[0-9](\\.[0-9]+)?S)?'$"</xsl:text>
          </xsl:when>
          <xsl:when test="$odata-version='2.0'">
            <xsl:text>,"example":"PT15H51M04S"</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>,"format":"time"</xsl:text>
            <xsl:if test="not($inParameter)">
              <xsl:text>,"example":"15:51:04"</xsl:text>
            </xsl:if>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Duration'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
        <xsl:text>,"format":"duration"</xsl:text>
        <xsl:if test="not($inParameter)">
          <xsl:text>,"example":"P4DT15H51M04S"</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$singleType='Edm.PrimitiveType'">
        <xsl:call-template name="nullableType">
          <xsl:with-param name="type" select="'boolean,number,string'" />
          <xsl:with-param name="nullable" select="$nullable" />
          <xsl:with-param name="noArray" select="$noArray" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="$singleType='Edm.PropertyPath'">
        <xsl:text>"type":"object","properties":{"@odata.propertyPath":{"type":"string"}},"required":["@odata.propertyPath"]</xsl:text>
      </xsl:when>
      <xsl:when test="$singleType='Edm.GeographyPoint' or $singleType='Edm.GeometryPoint'">
        <xsl:if test="not($openapi-version='2.0') and (not($nullable='false') or $target/@DefaultValue)">
          <xsl:if test="not($nullable='false')">
            <xsl:text>"nullable":true,</xsl:text>
          </xsl:if>
          <xsl:text>"anyOf":[{</xsl:text>
        </xsl:if>
        <xsl:text>"$ref":"</xsl:text>
        <xsl:value-of select="$reuse-schemas" />
        <xsl:text>geoPoint"</xsl:text>
        <xsl:if test="not($openapi-version='2.0') and (not($nullable='false') or $target/@DefaultValue)">
          <xsl:text>}]</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$singleType='Edm.Untyped'">
        <xsl:if test="not($inParameter and $openapi-version='2.0')">
          <xsl:text>"example":{}</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="$qualifier='Edm'">
        <xsl:message>
          <xsl:text>TODO: inline </xsl:text>
          <xsl:value-of select="$singleType" />
        </xsl:message>
        <xsl:if test="not($openapi-version='2.0') and (not($nullable='false') or $target/@DefaultValue)">
          <xsl:if test="not($nullable='false')">
            <xsl:text>"nullable":true,</xsl:text>
          </xsl:if>
          <xsl:text>"anyOf":[{</xsl:text>
        </xsl:if>
        <xsl:text>"$ref":"</xsl:text>
        <xsl:value-of select="$odata-schema" />
        <xsl:text>#/definitions/</xsl:text>
        <xsl:value-of select="$singleType" />
        <xsl:text>"</xsl:text>
        <xsl:if test="not($openapi-version='2.0') and (not($nullable='false') or $target/@DefaultValue)">
          <xsl:text>}]</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:otherwise>
        <xsl:if test="not($openapi-version='2.0') and (not($nullable='false') or $target/@DefaultValue or $target/@MaxLength)">
          <xsl:if test="not($nullable='false')">
            <xsl:text>"nullable":true,</xsl:text>
          </xsl:if>
          <xsl:text>"anyOf":[{</xsl:text>
        </xsl:if>
        <xsl:call-template name="ref">
          <xsl:with-param name="qualifier" select="$qualifier" />
          <xsl:with-param name="name" select="$simpleName" />
          <xsl:with-param name="suffix" select="$suffix" />
        </xsl:call-template>
        <xsl:if test="not($openapi-version='2.0') and (not($nullable='false') or @DefaultValue or $target/@MaxLength)">
          <xsl:text>}]</xsl:text>
        </xsl:if>
        <xsl:apply-templates select="$target/@MaxLength" />
        <xsl:call-template name="Validation.Minimum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:call-template name="Validation.Maximum">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:apply-templates select="$target/@DefaultValue">
      <xsl:with-param name="type" select="$singleType" />
    </xsl:apply-templates>
    <xsl:if test="$collection">
      <xsl:if test="$odata-version='2.0'">
        <xsl:text>}}</xsl:text>
      </xsl:if>
      <xsl:text>}</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template name="Validation.AllowedValues">
    <xsl:param name="target" />
    <xsl:variable name="target-path">
      <xsl:call-template name="annotation-target">
        <xsl:with-param name="node" select="$target" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="target-path-aliased">
      <xsl:call-template name="annotation-target">
        <xsl:with-param name="node" select="$target" />
        <xsl:with-param name="qualifier" select="$target/ancestor::edm:Schema/@Alias" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="allowedValues"
      select="//edm:Annotations[(@Target=$target-path or @Target=$target-path-aliased)]/edm:Annotation[(@Term=concat($validationNamespace,'.AllowedValues') or @Term=concat($validationAlias,'.AllowedValues')) and not(@Qualifier)]
                                                                                       |edm:Annotation[(@Term=concat($validationNamespace,'.AllowedValues') or @Term=concat($validationAlias,'.AllowedValues')) and not(@Qualifier)]" />
    <xsl:if test="$allowedValues">
      <xsl:text>,"enum":[</xsl:text>
      <xsl:apply-templates select="$allowedValues/edm:Collection/edm:Record" mode="Validation.AllowedValues" />
      <xsl:text>]</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template name="Validation.Minimum">
    <xsl:param name="target" />
    <xsl:variable name="target-path">
      <xsl:call-template name="annotation-target">
        <xsl:with-param name="node" select="$target" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="target-path-aliased">
      <xsl:call-template name="annotation-target">
        <xsl:with-param name="node" select="$target" />
        <xsl:with-param name="qualifier" select="$target/ancestor::edm:Schema/@Alias" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="minimum"
      select="//edm:Annotations[(@Target=$target-path or @Target=$target-path-aliased)]/edm:Annotation[(@Term=concat($validationNamespace,'.Minimum') or @Term=concat($validationAlias,'.Minimum')) and not(@Qualifier)]
                                                                                       |edm:Annotation[(@Term=concat($validationNamespace,'.Minimum') or @Term=concat($validationAlias,'.Minimum')) and not(@Qualifier)]" />
    <xsl:if test="$minimum">
      <xsl:text>,"minimum":</xsl:text>
      <xsl:value-of select="$minimum/@Decimal|$minimum/edm:Decimal" />
      <xsl:variable name="exclusive"
        select="$minimum/edm:Annotation[(@Term=concat($validationNamespace,'.Exclusive') or @Term=concat($validationAlias,'.Exclusive')) and not(@Qualifier)]" />
      <xsl:if test="$exclusive/@Bool = 'true' or $exclusive/edm:Bool='true'">
        <xsl:text>,"exclusiveMinimum":true</xsl:text>
      </xsl:if>
    </xsl:if>
  </xsl:template>

  <xsl:template name="Validation.Maximum">
    <xsl:param name="target" />
    <xsl:variable name="target-path">
      <xsl:call-template name="annotation-target">
        <xsl:with-param name="node" select="$target" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="target-path-aliased">
      <xsl:call-template name="annotation-target">
        <xsl:with-param name="node" select="$target" />
        <xsl:with-param name="qualifier" select="$target/ancestor::edm:Schema/@Alias" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="maximum"
      select="//edm:Annotations[(@Target=$target-path or @Target=$target-path-aliased)]/edm:Annotation[(@Term=concat($validationNamespace,'.Maximum') or @Term=concat($validationAlias,'.Maximum')) and not(@Qualifier)]
                                                                                       |edm:Annotation[(@Term=concat($validationNamespace,'.Maximum') or @Term=concat($validationAlias,'.Maximum')) and not(@Qualifier)]" />
    <xsl:if test="$maximum">
      <xsl:text>,"maximum":</xsl:text>
      <xsl:value-of select="$maximum/@Decimal|$maximum/edm:Decimal" />
    </xsl:if>
    <xsl:variable name="exclusive"
      select="$maximum/edm:Annotation[(@Term=concat($validationNamespace,'.Exclusive') or @Term=concat($validationAlias,'.Exclusive')) and not(@Qualifier)]" />
    <xsl:if test="$exclusive/@Bool = 'true' or $exclusive/edm:Bool='true'">
      <xsl:text>,"exclusiveMaximum":true</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template name="Validation.Pattern">
    <xsl:param name="target" />
    <xsl:variable name="pattern">
      <xsl:call-template name="annotation-string">
        <xsl:with-param name="node" select="$target" />
        <xsl:with-param name="term" select="concat($validationNamespace,'.Pattern')" />
        <xsl:with-param name="termAliased" select="concat($validationAlias,'.Pattern')" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="$pattern!=''">
      <xsl:text>,"pattern":"</xsl:text>
      <xsl:value-of select="$pattern" />
      <xsl:text>"</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edm:Record" mode="Validation.AllowedValues">
    <xsl:if test="position()>1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>"</xsl:text>
    <xsl:value-of select="edm:PropertyValue[@Property='Value']/@String|edm:PropertyValue[@Property='Value']/edm:String" />
    <xsl:text>"</xsl:text>
  </xsl:template>

  <xsl:template name="ref">
    <xsl:param name="qualifier" />
    <xsl:param name="name" />
    <xsl:param name="suffix" select="null" />
    <xsl:variable name="internalNamespace"
      select="//edm:Schema[@Alias=$qualifier]/@Namespace|//edm:Schema[@Namespace=$qualifier]/@Namespace" />
    <xsl:choose>
      <xsl:when test="$internalNamespace">
        <xsl:text>"$ref":"</xsl:text>
        <xsl:value-of select="$reuse-schemas" />
        <xsl:value-of select="$internalNamespace" />
        <xsl:if test="not(//edm:Schema[@Namespace=$internalNamespace]/edm:*[@Name=$name])">
          <xsl:message>
            <xsl:text>Unknown type: </xsl:text>
            <xsl:value-of select="$qualifier" />
            <xsl:text>.</xsl:text>
            <xsl:value-of select="$name" />
          </xsl:message>
        </xsl:if>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>"$ref":"</xsl:text>
        <xsl:variable name="externalNamespace"
          select="//edmx:Include[@Alias=$qualifier]/@Namespace|//edmx:Include[@Namespace=$qualifier]/@Namespace" />
        <xsl:call-template name="json-url">
          <xsl:with-param name="url" select="//edmx:Include[@Namespace=$externalNamespace]/../@Uri" />
        </xsl:call-template>
        <xsl:value-of select="$reuse-schemas" />
        <xsl:value-of select="$externalNamespace" />
        <xsl:if test="not($externalNamespace)">
          <xsl:message>
            <xsl:text>Unknown qualifier: </xsl:text>
            <xsl:value-of select="$qualifier" />
            <xsl:text>Node: </xsl:text>
            <xsl:value-of select="local-name()" />
          </xsl:message>
        </xsl:if>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>.</xsl:text>
    <xsl:value-of select="$name" />
    <xsl:if
      test="//edm:Schema[@Namespace=$qualifier or @Alias=$qualifier]/edm:EntityType[@Name=$name]|//edm:Schema[@Namespace=$qualifier or @Alias=$qualifier]/edm:ComplexType[@Name=$name]"
    >
      <xsl:value-of select="$suffix" />
    </xsl:if>
    <xsl:text>"</xsl:text>
  </xsl:template>

  <xsl:template name="schema-ref">
    <xsl:param name="qualifiedName" />
    <xsl:param name="suffix" select="null" />
    <xsl:call-template name="ref">
      <xsl:with-param name="qualifier">
        <xsl:call-template name="substring-before-last">
          <xsl:with-param name="input" select="$qualifiedName" />
          <xsl:with-param name="marker" select="'.'" />
        </xsl:call-template>
      </xsl:with-param>
      <xsl:with-param name="name">
        <xsl:call-template name="substring-after-last">
          <xsl:with-param name="input" select="$qualifiedName" />
          <xsl:with-param name="marker" select="'.'" />
        </xsl:call-template>
      </xsl:with-param>
      <xsl:with-param name="suffix" select="$suffix" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="repeat">
    <xsl:param name="string" />
    <xsl:param name="count" />
    <xsl:if test="$count > 0">
      <xsl:value-of select="$string" />
    </xsl:if>
    <xsl:if test="$count > 1">
      <xsl:call-template name="repeat">
        <xsl:with-param name="string" select="$string" />
        <xsl:with-param name="count" select="$count - 1" />
      </xsl:call-template>
    </xsl:if>
  </xsl:template>

  <xsl:template name="nullableType">
    <xsl:param name="type" />
    <xsl:param name="nullable" />
    <xsl:param name="noArray" />
    <xsl:choose>
      <xsl:when test="$openapi-version='2.0'">
        <xsl:text>"type":</xsl:text>
        <xsl:if test="not($noArray) and (not($nullable='false') or contains($type,','))">
          <xsl:text>[</xsl:text>
        </xsl:if>
        <xsl:text>"</xsl:text>
        <xsl:choose>
          <xsl:when test="$noArray and contains($type,',')">
            <xsl:value-of select="substring-before($type,',')" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:call-template name="replace-all">
              <xsl:with-param name="string" select="$type" />
              <xsl:with-param name="old" select="','" />
              <xsl:with-param name="new" select="'&quot;,&quot;'" />
            </xsl:call-template>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:text>"</xsl:text>
        <xsl:if test="not($noArray) and not($nullable='false')">
          <xsl:text>,"null"</xsl:text>
        </xsl:if>
        <xsl:if test="not($noArray) and (not($nullable='false') or contains($type,','))">
          <xsl:text>]</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:otherwise>
        <xsl:choose>
          <xsl:when test="contains($type,',')">
            <xsl:text>"anyOf":[{"type":"</xsl:text>
            <xsl:call-template name="replace-all">
              <xsl:with-param name="string" select="$type" />
              <xsl:with-param name="old" select="','" />
              <xsl:with-param name="new" select="'&quot;},{&quot;type&quot;:&quot;'" />
            </xsl:call-template>
            <xsl:text>"}]</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>"type":"</xsl:text>
            <xsl:value-of select="$type" />
            <xsl:text>"</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:if test="not($nullable='false')">
          <xsl:text>,"nullable":true</xsl:text>
        </xsl:if>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="@MaxLength">
    <xsl:if test=".!='max'">
      <xsl:text>,"maxLength":</xsl:text>
      <xsl:choose>
        <xsl:when test="../@Type='Edm.Binary'">
          <xsl:value-of select="ceiling(4 * . div 3)" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="." />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:if>
  </xsl:template>

  <xsl:template match="@DefaultValue">
    <xsl:param name="type" />
    <xsl:text>,"default":</xsl:text>
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="$type" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="typeName">
      <xsl:call-template name="substring-after-last">
        <xsl:with-param name="input" select="$type" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="underlyingType">
      <xsl:choose>
        <xsl:when test="//edm:Schema[@Namespace=$qualifier]/edm:TypeDefinition[@Name=$typeName]/@UnderlyingType">
          <xsl:value-of select="//edm:Schema[@Namespace=$qualifier]/edm:TypeDefinition[@Name=$typeName]/@UnderlyingType" />
        </xsl:when>
        <xsl:when test="//edm:Schema[@Alias=$qualifier]/edm:TypeDefinition[@Name=$typeName]/@UnderlyingType">
          <xsl:value-of select="//edm:Schema[@Alias=$qualifier]/edm:TypeDefinition[@Name=$typeName]/@UnderlyingType" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$type" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="underlyingQualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="$underlyingType" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test=".='-INF' or .='INF' or .='NaN'">
        <xsl:text>"</xsl:text>
        <xsl:value-of select="." />
        <xsl:text>"</xsl:text>
      </xsl:when>
      <xsl:when test="$underlyingType='Edm.Boolean' and (.='true' or .='false' or .='null')">
        <xsl:value-of select="." />
      </xsl:when>
      <xsl:when
        test="($underlyingType='Edm.Decimal' or $underlyingType='Edm.Double' or $underlyingType='Edm.Single'
              or $underlyingType='Edm.Byte' or $underlyingType='Edm.SByte' or $underlyingType='Edm.Int16' or $underlyingType='Edm.Int32' or $underlyingType='Edm.Int64') and .=number(.)"
      >
        <xsl:value-of select="." />
      </xsl:when>
      <!-- FAKE: couldn't determine underlying primitive type, so guess from value -->
      <xsl:when test="$underlyingQualifier!='Edm' and (.='true' or .='false' or .='null' or .=number(.))">
        <xsl:value-of select="." />
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>"</xsl:text>
        <xsl:call-template name="escape">
          <xsl:with-param name="string" select="." />
        </xsl:call-template>
        <xsl:text>"</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="edm:EntityContainer" mode="paths">
    <xsl:apply-templates select="edm:EntitySet|edm:Singleton|edm:FunctionImport|edm:ActionImport" mode="list" />

    <xsl:variable name="batch-supported">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'BatchSupported'" />
        <xsl:with-param name="target" select="." />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="not($batch-supported='false')">
      <xsl:call-template name="batch" />
    </xsl:if>
  </xsl:template>

  <xsl:template name="batch">
    <xsl:if test="edm:EntitySet|edm:Singleton|edm:FunctionImport|edm:ActionImport">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>"/$batch":{"post":{"summary": "Send a group of requests","description": "Group multiple requests into a single request payload</xsl:text>
    <xsl:text>, see [OData Batch Requests](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_BatchRequests).</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>\n\n*Please note that \"Try it out\" is not supported for this request.*</xsl:text>
    </xsl:if>
    <xsl:text>","tags":["Batch Requests"],</xsl:text>
    <xsl:if test="$openapi-version='2.0'">
      <xsl:text>"consumes":["multipart/mixed;boundary=request-separator"],"produces":["multipart/mixed"],</xsl:text>
    </xsl:if>

    <xsl:choose>
      <xsl:when test="$openapi-version='2.0'">
        <xsl:text>"parameters":[{"name":"requestBody","in":"body",</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>"requestBody":{"required":true,</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>"description":"Batch request",</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>"content":{"multipart/mixed;boundary=request-separator":{</xsl:text>
    </xsl:if>
    <xsl:text>"schema":{"type":"string"</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>}</xsl:text>
    </xsl:if>
    <xsl:text>,"example":"--request-separator\nContent-Type: application/http\nContent-Transfer-Encoding: binary\n\nGET </xsl:text>
    <xsl:value-of select="//edm:EntitySet[1]/@Name" />
    <xsl:text> HTTP/1.1\nAccept: application/json\n\n\n--request-separator--"}</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>}</xsl:text>
    </xsl:if>
    <xsl:choose>
      <xsl:when test="$openapi-version='2.0'">
        <xsl:text>}]</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>}</xsl:text>
      </xsl:otherwise>
    </xsl:choose>

    <xsl:text>,"responses":{"202":{"description":"Batch response",</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>"content":{"multipart/mixed":{</xsl:text>
    </xsl:if>
    <xsl:text>"schema":{"type":"string"</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>}</xsl:text>
    </xsl:if>
    <xsl:text>,"example": "--response-separator\nContent-Type: application/http\n\nHTTP/1.1 200 OK\nContent-Type: application/json\n\n{...}\n--response-separator--"}</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>}</xsl:text>
    </xsl:if>
    <xsl:text>},</xsl:text>
    <xsl:value-of select="$defaultResponse" />
    <xsl:text>}}}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:EntitySet|edm:Singleton" mode="tags">
    <xsl:if test="position() = 1">
      <xsl:text>,"tags":[</xsl:text>
    </xsl:if>
    <xsl:if test="position()>1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>{"name":"</xsl:text>
    <xsl:value-of select="@Name" />

    <xsl:variable name="description">
      <xsl:call-template name="Core.Description">
        <xsl:with-param name="node" select="." />
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test="$description!=''">
        <xsl:text>","description":"</xsl:text>
        <xsl:value-of select="$description" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:variable name="type-description">
          <xsl:choose>
            <xsl:when test="local-name()='EntitySet'">
              <xsl:variable name="qualifier">
                <xsl:call-template name="substring-before-last">
                  <xsl:with-param name="input" select="@EntityType" />
                  <xsl:with-param name="marker" select="'.'" />
                </xsl:call-template>
              </xsl:variable>
              <xsl:variable name="namespace">
                <xsl:choose>
                  <xsl:when test="//edm:Schema[@Alias=$qualifier]">
                    <xsl:value-of select="//edm:Schema[@Alias=$qualifier]/@Namespace" />
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:value-of select="$qualifier" />
                  </xsl:otherwise>
                </xsl:choose>
              </xsl:variable>
              <xsl:variable name="type">
                <xsl:call-template name="substring-after-last">
                  <xsl:with-param name="input" select="@EntityType" />
                  <xsl:with-param name="marker" select="'.'" />
                </xsl:call-template>
              </xsl:variable>
              <xsl:variable name="entityType" select="//edm:Schema[@Namespace=$namespace]/edm:EntityType[@Name=$type]" />
              <xsl:call-template name="Core.Description">
                <xsl:with-param name="node" select="$entityType" />
              </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
              <!-- TODO: fall back to type text for singleton -->
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:if test="$type-description!=''">
          <xsl:text>","description":"</xsl:text>
          <xsl:value-of select="$type-description" />
        </xsl:if>
      </xsl:otherwise>
    </xsl:choose>

    <xsl:text>"}</xsl:text>
    <xsl:if test="position() = last()">
      <xsl:text>]</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edm:EntitySet">
    <xsl:apply-templates select="." mode="entitySet" />
    <xsl:apply-templates select="." mode="entity" />
  </xsl:template>

  <xsl:template match="edm:EntitySet" mode="entitySet">
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="@EntityType" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="namespace">
      <xsl:choose>
        <xsl:when test="//edm:Schema[@Alias=$qualifier]">
          <xsl:value-of select="//edm:Schema[@Alias=$qualifier]/@Namespace" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$qualifier" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="type">
      <xsl:call-template name="substring-after-last">
        <xsl:with-param name="input" select="@EntityType" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="qualifiedType">
      <xsl:value-of select="$namespace" />
      <xsl:text>.</xsl:text>
      <xsl:value-of select="$type" />
    </xsl:variable>
    <xsl:variable name="entityType" select="//edm:Schema[@Namespace=$namespace]/edm:EntityType[@Name=$type]" />

    <xsl:text>"/</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>":{</xsl:text>

    <!-- GET -->
    <xsl:variable name="addressable" select="edm:Annotation[@Term='TODO.Addressable']/@Bool" />
    <xsl:variable name="resultContext"
      select="$entityType/edm:Annotation[@Term=concat($commonNamespace,'.ResultContext') or @Term=concat($commonAlias,'.ResultContext')]" />
    <xsl:if test="not($addressable='false') and not($resultContext)">
      <xsl:text>"get":{</xsl:text>

      <xsl:call-template name="summary-description-qualified">
        <xsl:with-param name="node" select="." />
        <xsl:with-param name="qualifier" select="'Query'" />
        <xsl:with-param name="fallback-summary">
          <xsl:text>Get entities from </xsl:text>
          <xsl:value-of select="@Name" />
        </xsl:with-param>
      </xsl:call-template>

      <xsl:text>,"tags":["</xsl:text>
      <xsl:value-of select="@Name" />
      <xsl:text>"]</xsl:text>

      <xsl:text>,"parameters":[</xsl:text>

      <xsl:call-template name="query-options">
        <xsl:with-param name="after-keys" select="false()" />
        <xsl:with-param name="target" select="." />
        <xsl:with-param name="collection" select="true()" />
        <xsl:with-param name="entityType" select="$entityType" />
      </xsl:call-template>

      <xsl:text>]</xsl:text>

      <xsl:variable name="delta">
        <xsl:call-template name="capability">
          <xsl:with-param name="term" select="'ChangeTracking'" />
          <xsl:with-param name="property" select="'Supported'" />
        </xsl:call-template>
      </xsl:variable>

      <xsl:call-template name="responses">
        <xsl:with-param name="code" select="'200'" />
        <xsl:with-param name="type" select="concat('Collection(',$qualifiedType,')')" />
        <xsl:with-param name="delta" select="$delta" />
        <xsl:with-param name="description" select="'Retrieved entities'" />
      </xsl:call-template>

      <xsl:text>}</xsl:text>
    </xsl:if>

    <!-- POST -->
    <xsl:variable name="insertable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'InsertRestrictions'" />
        <xsl:with-param name="property" select="'Insertable'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="not($addressable='false') and not($resultContext) and not($insertable='false')">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:if test="not($insertable='false')">
      <xsl:text>"post":{</xsl:text>

      <xsl:call-template name="summary-description-qualified">
        <xsl:with-param name="node" select="." />
        <xsl:with-param name="qualifier" select="'Create'" />
        <xsl:with-param name="fallback-summary">
          <xsl:text>Add new entity to </xsl:text>
          <xsl:value-of select="@Name" />
        </xsl:with-param>
      </xsl:call-template>

      <xsl:text>,"tags":["</xsl:text>
      <xsl:value-of select="@Name" />
      <xsl:text>"],</xsl:text>

      <xsl:choose>
        <xsl:when test="$openapi-version='2.0'">
          <xsl:text>"parameters":[{"name":"</xsl:text>
          <xsl:value-of select="$type" />
          <xsl:text>","in":"body",</xsl:text>
        </xsl:when>
        <xsl:otherwise>
          <xsl:text>"requestBody":{"required":true,</xsl:text>
        </xsl:otherwise>
      </xsl:choose>
      <xsl:call-template name="entityTypeDescription">
        <xsl:with-param name="entityType" select="$entityType" />
        <xsl:with-param name="default" select="'New entity'" />
      </xsl:call-template>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>"content":{"application/json":{</xsl:text>
      </xsl:if>
      <xsl:text>"schema":{</xsl:text>
      <xsl:call-template name="schema-ref">
        <xsl:with-param name="qualifiedName" select="$qualifiedType" />
        <xsl:with-param name="suffix" select="'-create'" />
      </xsl:call-template>
      <xsl:text>}</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>}}</xsl:text>
      </xsl:if>
      <xsl:choose>
        <xsl:when test="$openapi-version='2.0'">
          <xsl:text>}]</xsl:text>
        </xsl:when>
        <xsl:otherwise>
          <xsl:text>}</xsl:text>
        </xsl:otherwise>
      </xsl:choose>

      <xsl:call-template name="responses">
        <xsl:with-param name="code" select="'201'" />
        <xsl:with-param name="type" select="$qualifiedType" />
        <xsl:with-param name="description" select="'Created entity'" />
      </xsl:call-template>
      <xsl:text>}</xsl:text>
    </xsl:if>

    <xsl:text>}</xsl:text>

    <xsl:variable name="qualifiedCollection" select="concat('Collection(',$qualifiedType,')')" />
    <xsl:variable name="aliasQualifiedCollection" select="concat('Collection(',$qualifiedType,')')" />
    <xsl:apply-templates
      select="//edm:Function[@IsBound='true' and (edm:Parameter[1]/@Type=$qualifiedCollection or edm:Parameter[1]/@Type=$aliasQualifiedCollection)]"
      mode="bound"
    >
      <!-- bound to entity set works as bound to singleton -->
      <xsl:with-param name="singleton" select="@Name" />
      <xsl:with-param name="entityType" select="$entityType" />
    </xsl:apply-templates>
    <xsl:apply-templates
      select="//edm:Action[@IsBound='true' and (edm:Parameter[1]/@Type=$qualifiedCollection or edm:Parameter[1]/@Type=$aliasQualifiedCollection)]"
      mode="bound"
    >
      <!-- bound to entity set works as bound to singleton -->
      <xsl:with-param name="singleton" select="@Name" />
      <xsl:with-param name="entityType" select="$entityType" />
    </xsl:apply-templates>
  </xsl:template>

  <xsl:template name="filter-RequiredProperties">
    <xsl:param name="target" select="." />
    <xsl:variable name="target-path" select="concat($target/../../@Namespace,'.',$target/../@Name,'/',$target/@Name)" />
    <xsl:variable name="target-path-aliased" select="concat($target/../../@Alias,'.',$target/../@Name,'/',$target/@Name)" />
    <xsl:variable name="target-node" select="//edm:Annotations[(@Target=$target-path or @Target=$target-path-aliased)]|$target" />
    <xsl:variable name="filter-restrictions"
      select="$target-node/edm:Annotation[@Term=concat($capabilitiesNamespace,'.FilterRestrictions') or @Term=concat($capabilitiesAlias,'.FilterRestrictions')]" />
    <xsl:variable name="required-properties"
      select="$filter-restrictions/edm:Record/edm:PropertyValue[@Property='RequiredProperties']/edm:Collection/edm:PropertyPath" />
    <xsl:apply-templates select="$required-properties" mode="filter-RequiredProperties" />
  </xsl:template>

  <xsl:template match="edm:PropertyPath" mode="filter-RequiredProperties">
    <xsl:if test="position()=1">
      <xsl:text>\n\nRequired filter properties:</xsl:text>
    </xsl:if>
    <xsl:text>\n- </xsl:text>
    <xsl:value-of select="." />
  </xsl:template>

  <xsl:template match="edm:Property" mode="orderby">
    <xsl:param name="after" select="'something'" />
    <xsl:if test="position()=1">
      <xsl:if test="$after">
        <xsl:text>,</xsl:text>
      </xsl:if>
      <xsl:text>{"name":"</xsl:text>
      <xsl:value-of select="$option-prefix" />
      <xsl:text>orderby","in":"query","description":"Order items by property values</xsl:text>
      <xsl:text>, see [OData Sorting](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionorderby)",</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>"explode":false,"schema":{</xsl:text>
      </xsl:if>
      <xsl:text>"type":"array","uniqueItems":true,"items":{"type":"string","enum":[</xsl:text>
    </xsl:if>
    <xsl:if test="position()>1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>"</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>","</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text> desc"</xsl:text>
    <xsl:if test="position()=last()">
      <xsl:text>]}}</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>}</xsl:text>
      </xsl:if>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edm:Property|edm:NavigationProperty" mode="select">
    <xsl:param name="after" select="'something'" />
    <xsl:if test="position()=1">
      <xsl:if test="$after">
        <xsl:text>,</xsl:text>
      </xsl:if>
      <xsl:text>{"name":"</xsl:text>
      <xsl:value-of select="$option-prefix" />
      <xsl:text>select","in":"query","description":"Select properties to be returned</xsl:text>
      <xsl:text>, see [OData Select](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionselect)",</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>"explode":false,"schema":{</xsl:text>
      </xsl:if>
      <xsl:text>"type":"array","uniqueItems":true,"items":{"type":"string","enum":[</xsl:text>
    </xsl:if>
    <xsl:if test="position()>1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>"</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>"</xsl:text>
    <xsl:if test="position()=last()">
      <xsl:text>]}}</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>}</xsl:text>
      </xsl:if>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edm:Property|edm:NavigationProperty" mode="expand">
    <xsl:param name="after" select="'something'" />
    <xsl:if test="position()=1">
      <xsl:if test="$after">
        <xsl:text>,</xsl:text>
      </xsl:if>
      <xsl:text>{"name":"</xsl:text>
      <xsl:value-of select="$option-prefix" />
      <xsl:text>expand","in":"query","description":"Expand related entities</xsl:text>
      <xsl:text>, see [OData Expand](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionexpand)",</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>"explode":false,"schema":{</xsl:text>
      </xsl:if>
      <xsl:text>"type":"array","uniqueItems":true,"items":{"type":"string","enum":[</xsl:text>
      <xsl:if test="$odata-version!='2.0'">
        <xsl:text>"*",</xsl:text>
      </xsl:if>
    </xsl:if>
    <xsl:if test="position()!=1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>"</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>"</xsl:text>
    <xsl:if test="position()=last()">
      <xsl:text>]}}</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>}</xsl:text>
      </xsl:if>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edm:EntitySet" mode="entity">
    <xsl:variable name="indexable">
      <xsl:call-template name="capability-indexablebykey" />
    </xsl:variable>
    <xsl:if test="not($indexable='false')">

      <xsl:variable name="qualifier">
        <xsl:call-template name="substring-before-last">
          <xsl:with-param name="input" select="@EntityType" />
          <xsl:with-param name="marker" select="'.'" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:variable name="namespace">
        <xsl:choose>
          <xsl:when test="//edm:Schema[@Alias=$qualifier]">
            <xsl:value-of select="//edm:Schema[@Alias=$qualifier]/@Namespace" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="$qualifier" />
          </xsl:otherwise>
        </xsl:choose>
      </xsl:variable>
      <xsl:variable name="type">
        <xsl:call-template name="substring-after-last">
          <xsl:with-param name="input" select="@EntityType" />
          <xsl:with-param name="marker" select="'.'" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:variable name="qualifiedType">
        <xsl:value-of select="$namespace" />
        <xsl:text>.</xsl:text>
        <xsl:value-of select="$type" />
      </xsl:variable>
      <xsl:variable name="aliasQualifiedType">
        <xsl:value-of select="//edm:Schema[@Namespace=$namespace]/@Alias" />
        <xsl:text>.</xsl:text>
        <xsl:value-of select="$type" />
      </xsl:variable>
      <xsl:variable name="entityType" select="//edm:Schema[@Namespace=$namespace]/edm:EntityType[@Name=$type]" />

      <!-- entity path template -->
      <xsl:text>,"/</xsl:text>
      <xsl:value-of select="@Name" />
      <xsl:apply-templates select="$entityType" mode="key-in-path" />
      <xsl:text>":{</xsl:text>

      <!-- GET -->
      <xsl:variable name="addressable" select="edm:Annotation[@Term='TODO.Addressable']/@Bool" />

      <xsl:variable name="resultContext"
        select="$entityType/edm:Annotation[@Term=concat($commonNamespace,'.ResultContext') or @Term=concat($commonAlias,'.ResultContext')]" />
      <!-- indexable=true or indexable=default or -->
      <xsl:if test="not($addressable='false' and $indexable!='true') and not($resultContext)">
        <xsl:text>"get":{</xsl:text>

        <xsl:call-template name="summary-description-qualified">
          <xsl:with-param name="node" select="." />
          <xsl:with-param name="qualifier" select="'Read'" />
          <xsl:with-param name="fallback-summary">
            <xsl:text>Get entity from </xsl:text>
            <xsl:value-of select="@Name" />
            <xsl:text> by key</xsl:text>
          </xsl:with-param>
        </xsl:call-template>

        <xsl:text>,"tags":["</xsl:text>
        <xsl:value-of select="@Name" />
        <xsl:text>"]</xsl:text>
        <xsl:text>,"parameters":[</xsl:text>
        <xsl:apply-templates select="$entityType" mode="parameter" />

        <xsl:variable name="selectable">
          <xsl:call-template name="capability">
            <xsl:with-param name="term" select="'SelectSupport'" />
            <xsl:with-param name="property" select="'Supported'" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:if test="not($selectable='false')">
          <xsl:apply-templates
            select="$entityType/edm:Property|$entityType/edm:NavigationProperty[$odata-version='2.0']" mode="select" />
        </xsl:if>

        <xsl:variable name="expandable">
          <xsl:call-template name="capability">
            <xsl:with-param name="term" select="'ExpandRestrictions'" />
            <xsl:with-param name="property" select="'Expandable'" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:if test="not($expandable='false')">
          <xsl:apply-templates
            select="$entityType/edm:NavigationProperty|$entityType/edm:Property[@Type='Edm.Stream' and /edmx:Edmx/@Version='4.01']"
            mode="expand" />
        </xsl:if>
        <xsl:text>]</xsl:text>

        <xsl:call-template name="responses">
          <xsl:with-param name="type" select="$qualifiedType" />
          <xsl:with-param name="description" select="'Retrieved entity'" />
        </xsl:call-template>
        <xsl:text>}</xsl:text>
      </xsl:if>

      <!-- PATCH -->
      <xsl:variable name="updatable">
        <xsl:call-template name="capability">
          <xsl:with-param name="term" select="'UpdateRestrictions'" />
          <xsl:with-param name="property" select="'Updatable'" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:if test="not($addressable='false' and $indexable!='true') and not($resultContext) and not($updatable='false')">
        <xsl:text>,</xsl:text>
      </xsl:if>
      <xsl:if test="not($updatable='false')">
        <xsl:text>"patch":{</xsl:text>

        <xsl:call-template name="summary-description-qualified">
          <xsl:with-param name="node" select="." />
          <xsl:with-param name="qualifier" select="'Update'" />
          <xsl:with-param name="fallback-summary">
            <xsl:text>Update entity in </xsl:text>
            <xsl:value-of select="@Name" />
          </xsl:with-param>
        </xsl:call-template>

        <xsl:text>,"tags":["</xsl:text>
        <xsl:value-of select="@Name" />
        <xsl:text>"],</xsl:text>

        <xsl:text>"parameters":[</xsl:text>
        <xsl:apply-templates select="$entityType" mode="parameter" />

        <xsl:choose>
          <xsl:when test="$openapi-version='2.0'">
            <xsl:text>,{"name":"</xsl:text>
            <xsl:value-of select="$type" />
            <xsl:text>","in":"body",</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>],"requestBody":{"required":true,</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:call-template name="entityTypeDescription">
          <xsl:with-param name="entityType" select="$entityType" />
          <xsl:with-param name="default" select="'New property values'" />
        </xsl:call-template>
        <xsl:if test="$openapi-version!='2.0'">
          <xsl:text>"content":{"application/json":{</xsl:text>
        </xsl:if>
        <xsl:text>"schema":{</xsl:text>
        <xsl:if test="$odata-version='2.0'">
          <xsl:text>"title":"Modified </xsl:text>
          <xsl:value-of select="$type" />
          <xsl:text>","type":"object","properties":{"d":{</xsl:text>
        </xsl:if>
        <xsl:call-template name="schema-ref">
          <xsl:with-param name="qualifiedName" select="$qualifiedType" />
          <xsl:with-param name="suffix" select="'-update'" />
        </xsl:call-template>
        <xsl:if test="$odata-version='2.0'">
          <xsl:text>}}</xsl:text>
        </xsl:if>
        <xsl:choose>
          <xsl:when test="$openapi-version='2.0'">
            <xsl:text>}}]</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>}}}}</xsl:text>
          </xsl:otherwise>
        </xsl:choose>

        <xsl:call-template name="responses" />

        <xsl:text>}</xsl:text>
      </xsl:if>

      <!-- DELETE -->
      <xsl:variable name="deletable">
        <xsl:call-template name="capability">
          <xsl:with-param name="term" select="'DeleteRestrictions'" />
          <xsl:with-param name="property" select="'Deletable'" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:if
        test="((not($addressable='false' and $indexable!='true') and not($resultContext)) or not($updatable='false')) and not($deletable='false')"
      >
        <xsl:text>,</xsl:text>
      </xsl:if>
      <xsl:if test="not($deletable='false')">
        <xsl:text>"delete":{</xsl:text>

        <xsl:call-template name="summary-description-qualified">
          <xsl:with-param name="node" select="." />
          <xsl:with-param name="qualifier" select="'Delete'" />
          <xsl:with-param name="fallback-summary">
            <xsl:text>Delete entity from </xsl:text>
            <xsl:value-of select="@Name" />
          </xsl:with-param>
        </xsl:call-template>

        <xsl:text>,"tags":["</xsl:text>
        <xsl:value-of select="@Name" />
        <xsl:text>"]</xsl:text>
        <xsl:text>,"parameters":[</xsl:text>
        <xsl:apply-templates select="$entityType" mode="parameter" />
        <xsl:call-template name="if-match" />
        <xsl:text>]</xsl:text>
        <xsl:call-template name="responses" />
        <xsl:text>}</xsl:text>
      </xsl:if>

      <xsl:text>}</xsl:text>

      <xsl:apply-templates
        select="//edm:Function[@IsBound='true' and (edm:Parameter[1]/@Type=$qualifiedType or edm:Parameter[1]/@Type=$aliasQualifiedType)]"
        mode="bound"
      >
        <xsl:with-param name="entitySet" select="@Name" />
        <xsl:with-param name="entityType" select="$entityType" />
      </xsl:apply-templates>
      <xsl:apply-templates
        select="//edm:Action[@IsBound='true' and (edm:Parameter[1]/@Type=$qualifiedType or edm:Parameter[1]/@Type=$aliasQualifiedType)]"
        mode="bound"
      >
        <xsl:with-param name="entitySet" select="@Name" />
        <xsl:with-param name="entityType" select="$entityType" />
      </xsl:apply-templates>

      <xsl:apply-templates select="$entityType/edm:NavigationProperty" mode="pathItem">
        <xsl:with-param name="source" select="." />
        <xsl:with-param name="entityType" select="$entityType" />
        <xsl:with-param name="resultContext" select="$resultContext" />
      </xsl:apply-templates>

    </xsl:if>
  </xsl:template>

  <xsl:template name="if-match">
    <xsl:text>,{"name":"If-Match","in":"header","description":"ETag",</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>"schema":{</xsl:text>
    </xsl:if>
    <xsl:text>"type":"string"</xsl:text>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>}</xsl:text>
    </xsl:if>
    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:Singleton">
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="@Type" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="namespace">
      <xsl:choose>
        <xsl:when test="//edm:Schema[@Alias=$qualifier]">
          <xsl:value-of select="//edm:Schema[@Alias=$qualifier]/@Namespace" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$qualifier" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="type">
      <xsl:call-template name="substring-after-last">
        <xsl:with-param name="input" select="@Type" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="qualifiedType">
      <xsl:value-of select="$namespace" />
      <xsl:text>.</xsl:text>
      <xsl:value-of select="$type" />
    </xsl:variable>
    <xsl:variable name="aliasQualifiedType">
      <xsl:value-of select="//edm:Schema[@Namespace=$namespace]/@Alias" />
      <xsl:text>.</xsl:text>
      <xsl:value-of select="$type" />
    </xsl:variable>
    <xsl:variable name="entityType" select="//edm:Schema[@Namespace=$namespace]/edm:EntityType[@Name=$type]" />

    <!-- singleton path template -->
    <xsl:text>"/</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>":{</xsl:text>

    <!-- GET -->
    <xsl:text>"get":{</xsl:text>

    <xsl:call-template name="summary-description-qualified">
      <xsl:with-param name="node" select="." />
      <xsl:with-param name="qualifier" select="'Read'" />
      <xsl:with-param name="fallback-summary">
        <xsl:text>Get </xsl:text>
        <xsl:value-of select="@Name" />
      </xsl:with-param>
    </xsl:call-template>

    <xsl:text>,"tags":["</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>"]</xsl:text>
    <xsl:text>,"parameters":[</xsl:text>

    <xsl:variable name="delta">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'ChangeTracking'" />
        <xsl:with-param name="property" select="'Supported'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="$delta='true'">
      <!-- TODO: Prefer, Preference-Applied -->
    </xsl:if>

    <xsl:variable name="selectable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'SelectSupport'" />
        <xsl:with-param name="property" select="'Supported'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="selectable-properties"
      select="$entityType/edm:Property|$entityType/edm:NavigationProperty[$odata-version='2.0']" />
    <xsl:if test="not($selectable='false')">
      <!-- copy of select expression for selectable-properties - quick-fix for Java XSLT processor -->
      <xsl:apply-templates select="$entityType/edm:Property|$entityType/edm:NavigationProperty[$odata-version='2.0']"
        mode="select"
      >
        <!-- TODO: $delta='true' -->
        <xsl:with-param name="after" select="''" />
      </xsl:apply-templates>
    </xsl:if>

    <xsl:variable name="expandable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'ExpandRestrictions'" />
        <xsl:with-param name="property" select="'Expandable'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="not($expandable='false')">
      <xsl:apply-templates
        select="$entityType/edm:NavigationProperty|$entityType/edm:Property[@Type='Edm.Stream' and /edmx:Edmx/@Version='4.01']"
        mode="expand"
      >
        <!-- TODO: $delta='true' and -->
        <xsl:with-param name="after" select="not($selectable='false') and $selectable-properties" />
      </xsl:apply-templates>
    </xsl:if>

    <xsl:text>]</xsl:text>

    <xsl:call-template name="responses">
      <xsl:with-param name="type" select="$qualifiedType" />
      <xsl:with-param name="delta" select="$delta" />
      <xsl:with-param name="description" select="'Retrieved entity'" />
    </xsl:call-template>
    <xsl:text>}</xsl:text>


    <!-- PATCH -->
    <xsl:variable name="updatable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'UpdateRestrictions'" />
        <xsl:with-param name="property" select="'Updatable'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="not($updatable='false')">
      <xsl:text>,"patch":{</xsl:text>

      <xsl:call-template name="summary-description-qualified">
        <xsl:with-param name="node" select="." />
        <xsl:with-param name="qualifier" select="'Update'" />
        <xsl:with-param name="fallback-summary">
          <xsl:text>Update </xsl:text>
          <xsl:value-of select="@Name" />
        </xsl:with-param>
      </xsl:call-template>

      <xsl:text>,"tags":["</xsl:text>
      <xsl:value-of select="@Name" />
      <xsl:text>"],</xsl:text>

      <xsl:choose>
        <xsl:when test="$openapi-version='2.0'">
          <xsl:text>"parameters":[{"name":"</xsl:text>
          <xsl:value-of select="$type" />
          <xsl:text>","in":"body",</xsl:text>
        </xsl:when>
        <xsl:otherwise>
          <xsl:text>"requestBody":{"required":true,</xsl:text>
        </xsl:otherwise>
      </xsl:choose>
      <xsl:call-template name="entityTypeDescription">
        <xsl:with-param name="entityType" select="$entityType" />
        <xsl:with-param name="default" select="'New property values'" />
      </xsl:call-template>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>"content":{"application/json":{</xsl:text>
      </xsl:if>
      <xsl:text>"schema":{</xsl:text>
      <xsl:call-template name="schema-ref">
        <xsl:with-param name="qualifiedName" select="$qualifiedType" />
        <xsl:with-param name="suffix" select="'-update'" />
      </xsl:call-template>
      <xsl:text>}</xsl:text>
      <xsl:if test="$openapi-version!='2.0'">
        <xsl:text>}}</xsl:text>
      </xsl:if>
      <xsl:choose>
        <xsl:when test="$openapi-version='2.0'">
          <xsl:text>}]</xsl:text>
        </xsl:when>
        <xsl:otherwise>
          <xsl:text>}</xsl:text>
        </xsl:otherwise>
      </xsl:choose>

      <xsl:call-template name="responses" />

      <xsl:text>}</xsl:text>
    </xsl:if>

    <xsl:text>}</xsl:text>

    <xsl:apply-templates
      select="//edm:Function[@IsBound='true' and (edm:Parameter[1]/@Type=$qualifiedType or edm:Parameter[1]/@Type=$aliasQualifiedType)]"
      mode="bound"
    >
      <xsl:with-param name="singleton" select="@Name" />
      <xsl:with-param name="entityType" select="$entityType" />
    </xsl:apply-templates>
    <xsl:apply-templates
      select="//edm:Action[@IsBound='true' and (edm:Parameter[1]/@Type=$qualifiedType or edm:Parameter[1]/@Type=$aliasQualifiedType)]"
      mode="bound"
    >
      <xsl:with-param name="singleton" select="@Name" />
      <xsl:with-param name="entityType" select="$entityType" />
    </xsl:apply-templates>

    <xsl:apply-templates select="$entityType/edm:NavigationProperty" mode="pathItem">
      <xsl:with-param name="source" select="." />
      <xsl:with-param name="entityType" select="$entityType" />
    </xsl:apply-templates>
  </xsl:template>

  <xsl:template name="entityTypeDescription">
    <xsl:param name="entityType" />
    <xsl:param name="default" />
    <xsl:text>"description":"</xsl:text>
    <xsl:variable name="description">
      <xsl:call-template name="Core.Description">
        <xsl:with-param name="node" select="$entityType" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test="$description!=''">
        <xsl:value-of select="$description" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$default" />
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>",</xsl:text>
  </xsl:template>

  <xsl:template match="edm:EntityType" mode="key-in-path">
    <xsl:choose>
      <xsl:when test="edm:Key">
        <xsl:choose>
          <xsl:when test="$key-as-segment">
            <xsl:text>/</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:text>(</xsl:text>
          </xsl:otherwise>
        </xsl:choose>

        <xsl:apply-templates select="edm:Key/edm:PropertyRef" mode="key-in-path" />

        <xsl:if test="not($key-as-segment)">
          <xsl:text>)</xsl:text>
        </xsl:if>
      </xsl:when>
      <xsl:when test="@BaseType">
        <xsl:variable name="basetypeQualifier">
          <xsl:call-template name="substring-before-last">
            <xsl:with-param name="input" select="@BaseType" />
            <xsl:with-param name="marker" select="'.'" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="basetypeNamespace">
          <xsl:choose>
            <xsl:when test="//edm:Schema[@Alias=$basetypeQualifier]">
              <xsl:value-of select="//edm:Schema[@Alias=$basetypeQualifier]/@Namespace" />
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="$basetypeQualifier" />
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:variable name="basetype">
          <xsl:call-template name="substring-after-last">
            <xsl:with-param name="input" select="@BaseType" />
            <xsl:with-param name="marker" select="'.'" />
          </xsl:call-template>
        </xsl:variable>

        <xsl:apply-templates select="//edm:Schema[@Namespace=$basetypeNamespace]/edm:EntityType[@Name=$basetype]"
          mode="key-in-path" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:message>
          <xsl:text>ERROR: Entity type without key and without base type: </xsl:text>
          <xsl:value-of select="../@Namespace" />
          <xsl:text>.</xsl:text>
          <xsl:value-of select="@Name" />
        </xsl:message>
        <!-- produce valid json -->
        <xsl:text> - ERROR: neither key nor base type</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="edm:PropertyRef" mode="key-in-path">
    <xsl:variable name="name" select="@Name" />
    <xsl:variable name="type" select="../../edm:Property[@Name=$name]/@Type" />
    <xsl:if test="position()>1">
      <xsl:choose>
        <xsl:when test="$key-as-segment">
          <xsl:text>/</xsl:text>
        </xsl:when>
        <xsl:otherwise>
          <xsl:text>,</xsl:text>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:if>
    <xsl:if test="last()>1 and not($key-as-segment)">
      <xsl:value-of select="@Name" />
      <xsl:text>=</xsl:text>
    </xsl:if>
    <xsl:call-template name="pathValuePrefix">
      <xsl:with-param name="type" select="$type" />
    </xsl:call-template>
    <xsl:text>{</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>}</xsl:text>
    <xsl:call-template name="pathValueSuffix">
      <xsl:with-param name="type" select="$type" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="pathValuePrefix">
    <xsl:param name="type" />
    <xsl:choose>
      <xsl:when
        test="$type='Edm.Int64' or $type='Edm.Int32' or $type='Edm.Int16' or $type='Edm.SByte' or $type='Edm.Byte' or $type='Edm.Double' or $type='Edm.Single' or $type='Edm.Date' or $type='Edm.DateTimeOffset' or $type='Edm.Guid'" />
      <!-- TODO: handle other Edm types, enumeration types, and type definitions -->
      <xsl:otherwise>
        <xsl:if test="not($key-as-segment)">
          <xsl:text>'</xsl:text>
        </xsl:if>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="pathValueSuffix">
    <xsl:param name="type" />
    <xsl:choose>
      <xsl:when
        test="$type='Edm.Int64' or $type='Edm.Int32' or $type='Edm.Int16' or $type='Edm.SByte' or $type='Edm.Byte' or $type='Edm.Double' or $type='Edm.Single' or $type='Edm.Date' or $type='Edm.DateTimeOffset' or $type='Edm.Guid'" />
      <!-- TODO: handle other Edm types, enumeration types, and type definitions -->
      <xsl:otherwise>
        <xsl:if test="not($key-as-segment)">
          <xsl:text>'</xsl:text>
        </xsl:if>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="edm:EntityType" mode="parameter">
    <xsl:choose>
      <xsl:when test="edm:Key">
        <xsl:apply-templates select="edm:Key/edm:PropertyRef" mode="parameter" />
      </xsl:when>
      <xsl:when test="@BaseType">
        <xsl:variable name="basetypeQualifier">
          <xsl:call-template name="substring-before-last">
            <xsl:with-param name="input" select="@BaseType" />
            <xsl:with-param name="marker" select="'.'" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="basetypeNamespace">
          <xsl:choose>
            <xsl:when test="//edm:Schema[@Alias=$basetypeQualifier]">
              <xsl:value-of select="//edm:Schema[@Alias=$basetypeQualifier]/@Namespace" />
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="$basetypeQualifier" />
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:variable name="basetype">
          <xsl:call-template name="substring-after-last">
            <xsl:with-param name="input" select="@BaseType" />
            <xsl:with-param name="marker" select="'.'" />
          </xsl:call-template>
        </xsl:variable>

        <xsl:apply-templates select="//edm:Schema[@Namespace=$basetypeNamespace]/edm:EntityType[@Name=$basetype]"
          mode="parameter" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>"ERROR: entity type with neither key nor base type"</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="edm:PropertyRef" mode="parameter">
    <xsl:variable name="name" select="@Name" />
    <xsl:variable name="property" select="../../edm:Property[@Name=$name]" />
    <xsl:variable name="type" select="$property/@Type" />
    <xsl:if test="position()>1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>{"name":"</xsl:text>
    <xsl:value-of select="$name" />
    <xsl:text>","in":"path","required":true,"description":"</xsl:text>
    <xsl:variable name="description">
      <xsl:call-template name="description">
        <xsl:with-param name="node" select="$property" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test="$description!=''">
        <xsl:value-of select="$description" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>key: </xsl:text>
        <xsl:value-of select="$name" />
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>",</xsl:text>

    <xsl:choose>
      <xsl:when test="not($type)">
        <xsl:text>"x-error":"key property not found"</xsl:text>
        <xsl:message>
          <xsl:text>Key property </xsl:text>
          <xsl:value-of select="$name" />
          <xsl:text> not found for entity type </xsl:text>
          <xsl:value-of select="../../@Name" />
        </xsl:message>
      </xsl:when>
      <xsl:when test="$openapi-version='2.0'">
        <xsl:text>"type":</xsl:text>
        <xsl:choose>
          <xsl:when test="$type='Edm.Int64'">
            <xsl:text>"integer","format":"int64"</xsl:text>
          </xsl:when>
          <xsl:when test="$type='Edm.Int32'">
            <xsl:text>"integer","format":"int32"</xsl:text>
          </xsl:when>
          <!-- TODO: handle other Edm types, enumeration types, and type definitions -->
          <xsl:otherwise>
            <xsl:text>"string"</xsl:text>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>"schema":{</xsl:text>
        <xsl:call-template name="type">
          <xsl:with-param name="type" select="$type" />
          <xsl:with-param name="nullableFacet" select="'false'" />
          <xsl:with-param name="target" select="$property" />
        </xsl:call-template>
        <xsl:text>}</xsl:text>
      </xsl:otherwise>
    </xsl:choose>

    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:ActionImport">
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="@Action" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="namespace">
      <xsl:choose>
        <xsl:when test="//edm:Schema[@Alias=$qualifier]">
          <xsl:value-of select="//edm:Schema[@Alias=$qualifier]/@Namespace" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$qualifier" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="actionName">
      <xsl:call-template name="substring-after-last">
        <xsl:with-param name="input" select="@Action" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="action"
      select="//edm:Schema[@Namespace=$namespace]/edm:Action[@Name=$actionName and not(@IsBound='true')]" />

    <xsl:text>"/</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>":{"post":{</xsl:text>
    <xsl:call-template name="summary-description">
      <xsl:with-param name="node" select="." />
      <xsl:with-param name="node2" select="$action" />
      <xsl:with-param name="fallback-summary">
        <xsl:text>Invoke action </xsl:text>
        <xsl:value-of select="$action/@Name" />
      </xsl:with-param>
    </xsl:call-template>
    <xsl:text>,"tags":["</xsl:text>
    <xsl:choose>
      <xsl:when test="@EntitySet">
        <xsl:value-of select="@EntitySet" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>Service Operations</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>"]</xsl:text>
    <xsl:if test="$action/edm:Parameter">
      <xsl:choose>
        <xsl:when test="$odata-version='2.0'">
          <xsl:text>,"parameters":[</xsl:text>
          <xsl:apply-templates select="$action/edm:Parameter" mode="parameter" />
          <xsl:text>]</xsl:text>
        </xsl:when>
        <xsl:otherwise>
          <xsl:choose>
            <xsl:when test="$openapi-version='2.0'">
              <xsl:text>,"parameters":[{"name":"body","in":"body",</xsl:text>
            </xsl:when>
            <xsl:otherwise>
              <xsl:text>,"requestBody":{</xsl:text>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:text>"description":"Action parameters",</xsl:text>
          <xsl:if test="$openapi-version!='2.0'">
            <xsl:text>"content":{"application/json":{</xsl:text>
          </xsl:if>
          <xsl:text>"schema":{"type":"object"</xsl:text>
          <xsl:apply-templates select="$action/edm:Parameter" mode="hash">
            <xsl:with-param name="name" select="'properties'" />
          </xsl:apply-templates>
          <xsl:choose>
            <xsl:when test="$openapi-version='2.0'">
              <xsl:text>}}]</xsl:text>
            </xsl:when>
            <xsl:otherwise>
              <xsl:text>}}}}</xsl:text>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:if>

    <xsl:call-template name="responses">
      <xsl:with-param name="type" select="$action/edm:ReturnType/@Type" />
    </xsl:call-template>
    <xsl:text>}}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:FunctionImport">
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="@Function" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="namespace">
      <xsl:choose>
        <xsl:when test="//edm:Schema[@Alias=$qualifier]">
          <xsl:value-of select="//edm:Schema[@Alias=$qualifier]/@Namespace" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$qualifier" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="function">
      <xsl:call-template name="substring-after-last">
        <xsl:with-param name="input" select="@Function" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>

    <!-- need to apply templates for all function overloads that match the function name -->
    <xsl:apply-templates select="//edm:Schema[@Namespace=$namespace]/edm:Function[@Name=$function]" mode="import">
      <xsl:with-param name="functionImport" select="." />
    </xsl:apply-templates>
  </xsl:template>

  <xsl:template match="edm:Function" mode="import">
    <xsl:param name="functionImport" />

    <xsl:text>"/</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:if test="$odata-version!='2.0'">
      <xsl:text>(</xsl:text>
      <xsl:apply-templates select="edm:Parameter" mode="path" />
      <xsl:text>)</xsl:text>
    </xsl:if>
    <xsl:text>":{"get":{</xsl:text>
    <xsl:call-template name="summary-description">
      <xsl:with-param name="node" select="$functionImport" />
      <xsl:with-param name="node2" select="." />
      <xsl:with-param name="fallback-summary">
        <xsl:text>Invoke function </xsl:text>
        <xsl:value-of select="@Name" />
      </xsl:with-param>
    </xsl:call-template>
    <xsl:text>,"tags":["</xsl:text>
    <xsl:choose>
      <xsl:when test="$functionImport/@EntitySet">
        <xsl:value-of select="$functionImport/@EntitySet" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>Service Operations</xsl:text>
      </xsl:otherwise>
    </xsl:choose>

    <xsl:text>"],"parameters":[</xsl:text>
    <xsl:apply-templates select="edm:Parameter" mode="parameter" />
    <xsl:text>]</xsl:text>

    <xsl:call-template name="responses">
      <xsl:with-param name="type" select="edm:ReturnType/@Type" />
    </xsl:call-template>
    <xsl:text>}}</xsl:text>
  </xsl:template>

  <xsl:template name="query-options">
    <xsl:param name="after-keys" />
    <xsl:param name="target" />
    <xsl:param name="collection" />
    <xsl:param name="entityType" />

    <xsl:variable name="top-supported">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'TopSupported'" />
        <xsl:with-param name="target" select="$target" />
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="skip-supported">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'SkipSupported'" />
        <xsl:with-param name="target" select="$target" />
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="searchable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'SearchRestrictions'" />
        <xsl:with-param name="property" select="'Searchable'" />
        <xsl:with-param name="target" select="$target" />
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="filterable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'FilterRestrictions'" />
        <xsl:with-param name="property" select="'Filterable'" />
        <xsl:with-param name="target" select="$target" />
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="countable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'CountRestrictions'" />
        <xsl:with-param name="property" select="'Countable'" />
        <xsl:with-param name="target" select="$target" />
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="sortable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'SortRestrictions'" />
        <xsl:with-param name="property" select="'Sortable'" />
        <xsl:with-param name="target" select="$target" />
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="selectable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'SelectSupport'" />
        <xsl:with-param name="property" select="'Supported'" />
        <xsl:with-param name="target" select="$target" />
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="expandable">
      <xsl:call-template name="capability">
        <xsl:with-param name="term" select="'ExpandRestrictions'" />
        <xsl:with-param name="property" select="'Expandable'" />
        <xsl:with-param name="target" select="$target" />
      </xsl:call-template>
    </xsl:variable>

    <xsl:if test="$collection">

      <xsl:if test="not($top-supported='false')">
        <xsl:if test="$after-keys">
          <xsl:text>,</xsl:text>
        </xsl:if>
        <xsl:text>{"$ref":"</xsl:text>
        <xsl:value-of select="$reuse-parameters" />
        <xsl:text>top"}</xsl:text>
      </xsl:if>

      <xsl:if test="not($skip-supported='false')">
        <xsl:if test="$after-keys or not($top-supported='false')">
          <xsl:text>,</xsl:text>
        </xsl:if>
        <xsl:text>{"$ref":"</xsl:text>
        <xsl:value-of select="$reuse-parameters" />
        <xsl:text>skip"}</xsl:text>
      </xsl:if>

      <xsl:if test="not($searchable='false')">
        <xsl:if test="$after-keys or not($top-supported='false') or not($skip-supported='false')">
          <xsl:text>,</xsl:text>
        </xsl:if>
        <xsl:text>{"$ref":"</xsl:text>
        <xsl:value-of select="$reuse-parameters" />
        <xsl:text>search"}</xsl:text>
      </xsl:if>

      <xsl:if test="not($filterable='false')">
        <xsl:if test="$after-keys or not($top-supported='false') or not($skip-supported='false') or not($searchable='false')">
          <xsl:text>,</xsl:text>
        </xsl:if>
        <xsl:variable name="filter-required">
          <xsl:call-template name="capability">
            <xsl:with-param name="term" select="'FilterRestrictions'" />
            <xsl:with-param name="property" select="'RequiresFilter'" />
            <xsl:with-param name="target" select="$target" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:text>{"name":"</xsl:text>
        <xsl:value-of select="$option-prefix" />
        <xsl:text>filter","in":"query","description":"Filter items by property values</xsl:text>
        <xsl:text>, see [OData Filtering](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionfilter)</xsl:text>
        <xsl:call-template name="filter-RequiredProperties">
          <xsl:with-param name="target" select="$target" />
        </xsl:call-template>
        <xsl:text>",</xsl:text>
        <xsl:call-template name="parameter-type">
          <xsl:with-param name="type" select="'string'" />
        </xsl:call-template>
        <xsl:if test="$filter-required='true'">
          <xsl:text>,"required":true</xsl:text>
        </xsl:if>
        <xsl:text>}</xsl:text>
      </xsl:if>

      <xsl:if test="not($countable='false')">
        <xsl:if
          test="$after-keys or not($top-supported='false') or not($skip-supported='false') or not($searchable='false') or not($filterable='false')"
        >
          <xsl:text>,</xsl:text>
        </xsl:if>
        <xsl:text>{"$ref":"</xsl:text>
        <xsl:value-of select="$reuse-parameters" />
        <xsl:text>count"}</xsl:text>
      </xsl:if>

      <xsl:if test="not($sortable='false')">
        <xsl:variable name="non-sortable"
          select="$target/edm:Annotation[@Term=concat($capabilitiesNamespace,'.SortRestrictions') or @Term=concat($capabilitiesAlias,'.SortRestrictions')]/edm:Record/edm:PropertyValue[@Property='NonSortableProperties']/edm:Collection/edm:PropertyPath" />
        <xsl:apply-templates select="$entityType/edm:Property[not(@Name=$non-sortable)]" mode="orderby">
          <xsl:with-param name="after"
            select="$after-keys or not($top-supported='false') or not($skip-supported='false') or not($searchable='false') or not($filterable='false') or not($countable='false')" />
        </xsl:apply-templates>
      </xsl:if>

    </xsl:if>

    <xsl:variable name="selectable-properties"
      select="$entityType/edm:Property|$entityType/edm:NavigationProperty[$odata-version='2.0']" />
    <xsl:if test="not($selectable='false')">
      <!-- copy of select expression for selectable-properties - quick-fix for Java XSLT processor -->
      <xsl:apply-templates select="$entityType/edm:Property|$entityType/edm:NavigationProperty[$odata-version='2.0']"
        mode="select"
      >
        <xsl:with-param name="after"
          select="$after-keys or ($collection and (not($top-supported='false') or not($skip-supported='false') or not($searchable='false') or not($filterable='false') or not($countable='false') or not($sortable='false')))" />
      </xsl:apply-templates>
    </xsl:if>

    <xsl:if test="not($expandable='false')">
      <xsl:apply-templates
        select="$entityType/edm:NavigationProperty|$entityType/edm:Property[@Type='Edm.Stream' and /edmx:Edmx/@Version='4.01']"
        mode="expand"
      >
        <xsl:with-param name="after"
          select="$after-keys or ($collection and (not($top-supported='false') or not($skip-supported='false') or not($searchable='false') or not($filterable='false') or not($countable='false') or not($sortable='false') or (not($selectable='false') and $selectable-properties)))" />
      </xsl:apply-templates>
    </xsl:if>
  </xsl:template>

  <xsl:template name="responses">
    <xsl:param name="code" select="'200'" />
    <xsl:param name="type" select="null" />
    <xsl:param name="delta" select="'false'" />
    <xsl:param name="description" select="'Success'" />

    <xsl:variable name="collection" select="starts-with($type,'Collection(')" />

    <xsl:text>,"responses":{</xsl:text>
    <xsl:choose>
      <xsl:when test="not($type)">
        <xsl:text>"204":{"description":"</xsl:text>
        <xsl:value-of select="$description" />
        <xsl:text>"}</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>"</xsl:text>
        <xsl:value-of select="$code" />
        <xsl:text>":{"description":"</xsl:text>
        <xsl:value-of select="$description" />
        <xsl:text>",</xsl:text>
        <xsl:if test="$openapi-version!='2.0'">
          <xsl:text>"content":{"application/json":{</xsl:text>
        </xsl:if>
        <xsl:text>"schema":{</xsl:text>
        <xsl:if test="$collection or $odata-version='2.0'">
          <xsl:text>"title":"</xsl:text>
          <xsl:choose>
            <xsl:when test="$collection and $odata-version='2.0'">
              <xsl:text>Wrapper</xsl:text>
            </xsl:when>
            <xsl:when test="$collection">
              <xsl:text>Collection of </xsl:text>
              <xsl:call-template name="substring-after-last">
                <xsl:with-param name="input" select="substring-before(substring-after($type,'('),')')" />
                <xsl:with-param name="marker" select="'.'" />
              </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
              <xsl:call-template name="substring-after-last">
                <xsl:with-param name="input" select="$type" />
                <xsl:with-param name="marker" select="'.'" />
              </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:text>","type":"object","properties":{"</xsl:text>
          <xsl:choose>
            <xsl:when test="$odata-version='2.0'">
              <xsl:text>d</xsl:text>
            </xsl:when>
            <xsl:otherwise>
              <xsl:text>value</xsl:text>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:text>":{</xsl:text>
        </xsl:if>
        <xsl:if test="$delta='true' and not($collection)">
          <xsl:text>"allOf":[{</xsl:text>
        </xsl:if>
        <xsl:call-template name="type">
          <xsl:with-param name="type" select="$type" />
          <xsl:with-param name="nullableFacet" select="'false'" />
          <xsl:with-param name="inResponse" select="true()" />
        </xsl:call-template>
        <xsl:if test="$delta='true'">
          <xsl:text>},</xsl:text>
          <xsl:if test="not($collection)">
            <xsl:text>{"properties":{</xsl:text>
          </xsl:if>
          <xsl:text>"@</xsl:text>
          <!-- TODO: V2 only for collections: __delta next to results similar to __next, see http://services.odata.org/V2/Northwind/Northwind.svc/Customers -->
          <xsl:if test="/edmx:Edmx/@Version='4.0'">
            <xsl:text>odata.</xsl:text>
          </xsl:if>
          <xsl:text>deltaLink":{"type":"string","example":"</xsl:text>
          <xsl:value-of select="$basePath" />
          <xsl:text>/</xsl:text>
          <xsl:value-of select="@Name" />
          <xsl:text>?$deltatoken=opaque server-generated token for fetching the delta"</xsl:text>
          <xsl:if test="not($collection)">
            <xsl:text>}}}]</xsl:text>
          </xsl:if>
        </xsl:if>
        <xsl:if test="$collection or $odata-version='2.0'">
          <xsl:text>}}</xsl:text>
        </xsl:if>
        <xsl:if test="$openapi-version!='2.0'">
          <xsl:text>}}</xsl:text>
        </xsl:if>
        <xsl:text>}}</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>,</xsl:text>
    <xsl:value-of select="$defaultResponse" />
    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:NavigationProperty" mode="pathItem">
    <xsl:param name="source" />
    <xsl:param name="entityType" />
    <xsl:param name="resultContext" select="null" />

    <xsl:variable name="collection" select="starts-with(@Type,'Collection(')" />
    <xsl:variable name="name" select="@Name" />
    <xsl:variable name="bindingTarget" select="$source/edm:NavigationPropertyBinding[@Path=$name]/@Target" />
    <xsl:variable name="targetEntitySetName">
      <xsl:choose>
        <xsl:when
          test="contains($bindingTarget,'/') and substring-before($bindingTarget,'/') = concat($source/../../@Namespace,'.',$source/../@Name)"
        >
          <xsl:value-of select="substring-after($bindingTarget,'/')" />
        </xsl:when>
        <xsl:when
          test="contains($bindingTarget,'/') and substring-before($bindingTarget,'/') = concat($source/../../@Alias,'.',$source/../@Name)"
        >
          <xsl:value-of select="substring-after($bindingTarget,'/')" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$bindingTarget" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="targetSet" select="//edm:EntitySet[@Name=$targetEntitySetName]" />
    <xsl:variable name="targetAddressable" select="$targetSet/edm:Annotation[@Term='TODO.Addressable']/@Bool" />

    <!-- NavigationRestrictions on source -->
    <xsl:variable name="target-path" select="concat($source/../../@Namespace,'.',$source/../@Name,'/',$source/@Name)" />
    <xsl:variable name="target-path-aliased" select="concat($source/../../@Alias,'.',$source/../@Name,'/',$source/@Name)" />
    <xsl:variable name="navigationRestrictions"
      select="//edm:Annotations[(@Target=$target-path or @Target=$target-path-aliased)]/edm:Annotation[(@Term=concat($capabilitiesNamespace,'.NavigationRestrictions') or @Term=concat($capabilitiesAlias,'.NavigationRestrictions'))] 
                                                                               |$source/edm:Annotation[(@Term=concat($capabilitiesNamespace,'.NavigationRestrictions') or @Term=concat($capabilitiesAlias,'.NavigationRestrictions'))]" />
    <!-- NavigationRestrictions on source for this navigation property -->
    <xsl:variable name="restrictedProperties"
      select="$navigationRestrictions/edm:Record/edm:PropertyValue[@Property='RestrictedProperties']/edm:Collection" />
    <xsl:variable name="navPropName" select="@Name" />
    <xsl:variable name="navigationPropertyRestriction"
      select="$restrictedProperties/edm:Record[edm:PropertyValue[@Property='NavigationProperty']/@NavigationPropertyPath=$navPropName]" />
    <!-- navigability -->
    <xsl:variable name="sourceNavigability-pv"
      select="$navigationRestrictions/edm:Record/edm:PropertyValue[@Property='Navigability']" />
    <xsl:variable name="sourceNavigability"
      select="substring-after($sourceNavigability-pv/edm:EnumMember|$sourceNavigability-pv/@EnumMember,'/')" />
    <xsl:variable name="propertyNavigability-pv"
      select="$navigationPropertyRestriction/edm:PropertyValue[@Property='Navigability']" />
    <xsl:variable name="propertyNavigability"
      select="substring-after($propertyNavigability-pv/edm:EnumMember|$propertyNavigability-pv/@EnumMember,'/')" />
    <xsl:variable name="navigable"
      select="$propertyNavigability='Recursive' or $propertyNavigability='Single' 
              or (string-length($propertyNavigability)=0 and not($sourceNavigability='None'))" />

    <xsl:if test="$resultContext or @ContainsTarget='true' or ($navigable and not($targetAddressable='false'))">

      <xsl:variable name="nullable">
        <xsl:call-template name="nullableFacetValue">
          <xsl:with-param name="type" select="@Type" />
          <xsl:with-param name="nullableFacet" select="@Nullable" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:variable name="singleType">
        <xsl:choose>
          <xsl:when test="$collection">
            <xsl:value-of select="substring-before(substring-after(@Type,'('),')')" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="@Type" />
          </xsl:otherwise>
        </xsl:choose>
      </xsl:variable>
      <xsl:variable name="qualifier">
        <xsl:call-template name="substring-before-last">
          <xsl:with-param name="input" select="$singleType" />
          <xsl:with-param name="marker" select="'.'" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:variable name="targetNamespace">
        <xsl:choose>
          <xsl:when test="//edm:Schema[@Alias=$qualifier]">
            <xsl:value-of select="//edm:Schema[@Alias=$qualifier]/@Namespace" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="$qualifier" />
          </xsl:otherwise>
        </xsl:choose>
      </xsl:variable>
      <xsl:variable name="simpleName">
        <xsl:call-template name="substring-after-last">
          <xsl:with-param name="input" select="$singleType" />
          <xsl:with-param name="marker" select="'.'" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:variable name="targetType">
        <xsl:value-of select="$targetNamespace" />
        <xsl:text>.</xsl:text>
        <xsl:value-of select="$simpleName" />
      </xsl:variable>
      <xsl:variable name="targetEntityType"
        select="//edm:Schema[@Namespace=$targetNamespace]/edm:EntityType[@Name=$simpleName]" />

      <xsl:text>,"/</xsl:text>
      <xsl:value-of select="$source/@Name" />
      <xsl:apply-templates select="$entityType[local-name($source)='EntitySet']" mode="key-in-path" />
      <xsl:text>/</xsl:text>
      <xsl:value-of select="@Name" />
      <xsl:text>":{</xsl:text>

      <!-- GET -->
      <xsl:text>"get":{</xsl:text>

      <xsl:text>"summary":"Get related </xsl:text>
      <xsl:choose>
        <xsl:when test="not($collection)">
          <xsl:value-of select="$simpleName" />
        </xsl:when>
        <xsl:when test="$targetSet">
          <xsl:value-of select="$targetSet/@Name" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="@Name" />
        </xsl:otherwise>
      </xsl:choose>
      <xsl:text>","tags":["</xsl:text>
      <xsl:value-of select="$source/@Name" />
      <xsl:if test="not($resultContext) and $targetSet and $targetSet/@Name!=$source/@Name">
        <xsl:text>","</xsl:text>
        <xsl:value-of select="$targetSet/@Name" />
      </xsl:if>
      <xsl:text>"]</xsl:text>

      <xsl:text>,"parameters":[</xsl:text>
      <xsl:apply-templates select="$entityType[local-name($source)='EntitySet']" mode="parameter" />

      <xsl:call-template name="query-options">
        <xsl:with-param name="after-keys" select="local-name($source)='EntitySet'" />
        <xsl:with-param name="target" select="$targetSet" />
        <xsl:with-param name="collection" select="$collection" />
        <xsl:with-param name="entityType" select="$targetEntityType" />
      </xsl:call-template>

      <xsl:text>]</xsl:text>

      <xsl:call-template name="responses">
        <xsl:with-param name="code" select="'200'" />
        <xsl:with-param name="type" select="concat('Collection(',$targetType,')')" />
        <xsl:with-param name="description" select="'Retrieved entities'" />
      </xsl:call-template>

      <xsl:text>}</xsl:text>

      <!-- POST -->
      <xsl:if test="$collection and ($targetSet or @ContainsTarget='true')">
        <xsl:variable name="insertable">
          <xsl:call-template name="capability">
            <xsl:with-param name="term" select="'InsertRestrictions'" />
            <xsl:with-param name="property" select="'Insertable'" />
            <xsl:with-param name="target" select="$targetSet" />
          </xsl:call-template>
        </xsl:variable>

        <!-- InsertRestrictions on source for this navigation property -->
        <xsl:variable name="insertRestrictions"
          select="$navigationPropertyRestriction/edm:PropertyValue[@Property='InsertRestrictions']" />
        <xsl:variable name="navigation-insertable"
          select="$insertRestrictions/edm:Record/edm:PropertyValue[@Property='Insertable']/@Bool" />

        <xsl:if test="$navigation-insertable='true' or (not($navigation-insertable) and not($insertable='false'))">
          <xsl:text>,"post":{</xsl:text>

          <xsl:text>"summary":"Add related </xsl:text>
          <xsl:value-of select="$simpleName" />
          <xsl:text>","tags":["</xsl:text>
          <xsl:value-of select="$source/@Name" />
          <xsl:if test="not($resultContext) and $targetSet and $targetSet/@Name!=$source/@Name">
            <xsl:text>","</xsl:text>
            <xsl:value-of select="$targetSet/@Name" />
          </xsl:if>
          <xsl:text>"]</xsl:text>

          <xsl:text>,"parameters":[</xsl:text>
          <xsl:apply-templates select="$entityType[local-name($source)='EntitySet']" mode="parameter" />

          <xsl:choose>
            <xsl:when test="$openapi-version='2.0'">
              <xsl:if test="local-name($source)='EntitySet'">
                <xsl:text>,</xsl:text>
              </xsl:if>
              <xsl:text>{"name":"</xsl:text>
              <xsl:value-of select="$simpleName" />
              <xsl:text>","in":"body",</xsl:text>
            </xsl:when>
            <xsl:otherwise>
              <xsl:text>],"requestBody":{"required":true,</xsl:text>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:call-template name="entityTypeDescription">
            <xsl:with-param name="entityType" select="$targetEntityType" />
            <xsl:with-param name="default" select="'New entity'" />
          </xsl:call-template>
          <xsl:if test="$openapi-version!='2.0'">
            <xsl:text>"content":{"application/json":{</xsl:text>
          </xsl:if>
          <xsl:text>"schema":{</xsl:text>
          <xsl:call-template name="schema-ref">
            <xsl:with-param name="qualifiedName" select="$targetType" />
            <xsl:with-param name="suffix" select="'-create'" />
          </xsl:call-template>
          <xsl:text>}</xsl:text>
          <xsl:if test="$openapi-version!='2.0'">
            <xsl:text>}}</xsl:text>
          </xsl:if>
          <xsl:choose>
            <xsl:when test="$openapi-version='2.0'">
              <xsl:text>}]</xsl:text>
            </xsl:when>
            <xsl:otherwise>
              <xsl:text>}</xsl:text>
            </xsl:otherwise>
          </xsl:choose>

          <xsl:call-template name="responses">
            <xsl:with-param name="code" select="'201'" />
            <xsl:with-param name="type" select="$targetType" />
            <xsl:with-param name="description" select="'Created entity'" />
          </xsl:call-template>

          <xsl:text>}</xsl:text>
        </xsl:if>
      </xsl:if>

      <xsl:text>}</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edm:Action" mode="bound">
    <xsl:param name="entitySet" />
    <xsl:param name="singleton" />
    <xsl:param name="entityType" />

    <xsl:text>,"/</xsl:text>
    <xsl:choose>
      <xsl:when test="$entitySet">
        <xsl:value-of select="$entitySet" />
        <xsl:apply-templates select="$entityType" mode="key-in-path" />
      </xsl:when>
      <xsl:when test="$singleton">
        <xsl:value-of select="$singleton" />
      </xsl:when>
    </xsl:choose>
    <xsl:text>/</xsl:text>
    <xsl:choose>
      <xsl:when
        test="../edm:Annotation[(@Term=concat($coreNamespace,'.DefaultNamespace') or @Term=concat($coreAlias,'.DefaultNamespace')) and not(@Qualifier)]" />
      <xsl:when test="../@Alias">
        <xsl:value-of select="../@Alias" />
        <xsl:text>.</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="../@Namespace" />
        <xsl:text>.</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:value-of select="@Name" />
    <xsl:text>":{"post":{</xsl:text>
    <xsl:call-template name="summary-description">
      <xsl:with-param name="fallback-summary">
        <xsl:text>Invoke action </xsl:text>
        <xsl:value-of select="@Name" />
      </xsl:with-param>
    </xsl:call-template>
    <xsl:text>,"tags":["</xsl:text>
    <xsl:value-of select="$entitySet" />
    <xsl:value-of select="$singleton" />
    <xsl:text>"]</xsl:text>

    <xsl:if test="$entitySet or $openapi-version='2.0'">
      <xsl:text>,"parameters":[</xsl:text>
    </xsl:if>
    <xsl:if test="$entitySet">
      <xsl:apply-templates select="$entityType" mode="parameter" />
    </xsl:if>

    <xsl:choose>
      <xsl:when test="$openapi-version='2.0'">
        <xsl:if test="edm:Parameter[position()>1]">
          <xsl:if test="$entitySet">
            <xsl:text>,</xsl:text>
          </xsl:if>
          <xsl:text>{"name":"body","in":"body",</xsl:text>
          <xsl:text>"description":"Action parameters",</xsl:text>
          <xsl:text>"schema":{"type":"object"</xsl:text>
          <xsl:apply-templates select="edm:Parameter[position()>1]" mode="hash">
            <xsl:with-param name="name" select="'properties'" />
          </xsl:apply-templates>
          <xsl:text>}}</xsl:text>
        </xsl:if>
        <xsl:text>]</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:if test="$entitySet">
          <xsl:text>]</xsl:text>
        </xsl:if>
        <xsl:if test="edm:Parameter[position()>1]">
          <xsl:text>,"requestBody":{</xsl:text>
          <xsl:text>"description":"Action parameters",</xsl:text>
          <xsl:text>"content":{"application/json":{</xsl:text>
          <xsl:text>"schema":{"type":"object"</xsl:text>
          <xsl:apply-templates select="edm:Parameter[position()>1]" mode="hash">
            <xsl:with-param name="name" select="'properties'" />
          </xsl:apply-templates>
          <xsl:text>}}}}</xsl:text>
        </xsl:if>
      </xsl:otherwise>
    </xsl:choose>

    <xsl:call-template name="responses">
      <xsl:with-param name="type" select="edm:ReturnType/@Type" />
    </xsl:call-template>
    <xsl:text>}}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:Function" mode="bound">
    <xsl:param name="entitySet" />
    <xsl:param name="singleton" />
    <xsl:param name="entityType" />
    <xsl:variable name="singleReturnType">
      <xsl:choose>
        <xsl:when test="starts-with(edm:ReturnType/@Type,'Collection(')">
          <xsl:value-of select="substring-before(substring-after(edm:ReturnType/@Type,'('),')')" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="edm:ReturnType/@Type" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <xsl:text>,"/</xsl:text>
    <xsl:choose>
      <xsl:when test="$entitySet">
        <xsl:value-of select="$entitySet" />
        <xsl:apply-templates select="$entityType" mode="key-in-path" />
      </xsl:when>
      <xsl:when test="$singleton">
        <xsl:value-of select="$singleton" />
      </xsl:when>
    </xsl:choose>
    <xsl:text>/</xsl:text>
    <xsl:choose>
      <xsl:when
        test="../edm:Annotation[(@Term=concat($coreNamespace,'.DefaultNamespace') or @Term=concat($coreAlias,'.DefaultNamespace')) and not(@Qualifier)]" />
      <xsl:when test="../@Alias">
        <xsl:value-of select="../@Alias" />
        <xsl:text>.</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="../@Namespace" />
        <xsl:text>.</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:value-of select="@Name" />
    <xsl:text>(</xsl:text>
    <xsl:apply-templates select="edm:Parameter[position()>1]" mode="path" />
    <xsl:text>)":{"get":{</xsl:text>
    <xsl:call-template name="summary-description">
      <xsl:with-param name="fallback-summary">
        <xsl:text>Invoke function </xsl:text>
        <xsl:value-of select="@Name" />
      </xsl:with-param>
    </xsl:call-template>
    <xsl:text>,"tags":["</xsl:text>
    <xsl:value-of select="$entitySet" />
    <xsl:value-of select="$singleton" />
    <xsl:text>"],"parameters":[</xsl:text>
    <xsl:apply-templates select="$entityType[$entitySet]|edm:Parameter[position()>1]" mode="parameter" />
    <xsl:text>]</xsl:text>

    <xsl:call-template name="responses">
      <xsl:with-param name="type" select="edm:ReturnType/@Type" />
    </xsl:call-template>
    <xsl:text>}}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:Action/edm:Parameter" mode="hashvalue">
    <xsl:call-template name="type">
      <xsl:with-param name="type" select="@Type" />
      <xsl:with-param name="nullableFacet" select="@Nullable" />
    </xsl:call-template>
    <xsl:call-template name="title-description" />
  </xsl:template>

  <xsl:template match="edm:Action/edm:Parameter|edm:Function/edm:Parameter" mode="parameter">
    <xsl:if test="position() > 1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:text>{"name":"</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:choose>
      <xsl:when test="$odata-version='2.0'">
        <xsl:text>","in":"query",</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:text>","in":"path",</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
    <!-- only in V4 and if not nullable in V2 -->
    <xsl:if test="$odata-version!='2.0' or not(@Nullable='true')">
      <xsl:text>"required":true,</xsl:text>
    </xsl:if>
    <xsl:variable name="description">
      <xsl:call-template name="description">
        <xsl:with-param name="node" select="." />
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="hint">
      <xsl:if test="$odata-version='2.0'">
        <xsl:choose>
          <xsl:when test="@Type='Edm.Binary'">
            <xsl:text>Value needs to be in hex-pair format, enclosed in single quotes, and prefixed with `X`, e.g. `X'4F44617461'`</xsl:text>
          </xsl:when>
          <xsl:when test="@Type='Edm.Boolean'" />
          <xsl:when test="@Type='Edm.Byte'" />
          <xsl:when test="@Type='Edm.Date'"> <!-- Note: was Edm.DateTime in the V2 source XML -->
            <xsl:text>Value needs to be enclosed in single quotes and prefixed with `datetime`, e.g. `datetime'2017-12-31T00:00'`</xsl:text>
          </xsl:when>
          <xsl:when test="@Type='Edm.DateTimeOffset'">
            <xsl:text>Value needs to be enclosed in single quotes and prefixed with `datetimeoffset`, e.g. `datetimeoffset'2017-12-31T23:59:59Z'`</xsl:text>
          </xsl:when>
          <xsl:when test="@Type='Edm.TimeOfDay'">
            <xsl:text>Value needs to be in duration format, enclosed in single quotes, and prefixed with `time`, e.g. `time'PT23H59M59.999S'`</xsl:text>
          </xsl:when>
          <xsl:when test="@Type='Edm.Decimal' and @Scale>0">
            <xsl:text>Value needs to be suffixed with `M`</xsl:text>
          </xsl:when>
          <xsl:when test="@Type='Edm.Guid'">
            <xsl:text>Value needs to be enclosed in single quotes and prefixed with `guid`, e.g. `guid'01234567-0123-0123-0123-0123456789ab'`</xsl:text>
          </xsl:when>
          <xsl:when test="@Type='Edm.SByte'" />
          <xsl:when test="@Type='Edm.Int16'" />
          <xsl:when test="@Type='Edm.Int32'" />
          <xsl:when test="@Type='Edm.Int64'">
            <xsl:text>Value needs to be suffixed with `L`</xsl:text>
          </xsl:when>
          <xsl:when test="@Type='Edm.String'">
            <xsl:text>Value needs to be enclosed in single quotes</xsl:text>
          </xsl:when>
          <xsl:otherwise>
            <xsl:message>
              <xsl:text>Parameter of type </xsl:text>
              <xsl:value-of select="@Type" />
            </xsl:message>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:if>
    </xsl:variable>
    <xsl:if test="$description!='' or $hint!=''">
      <xsl:text>"description":"</xsl:text>
      <xsl:value-of select="$description" />
      <xsl:if test="$description!='' and $hint!=''">
        <xsl:text>  \n(</xsl:text>
      </xsl:if>
      <xsl:value-of select="$hint" />
      <xsl:if test="$description!='' and $hint!=''">
        <xsl:text>)</xsl:text>
      </xsl:if>
      <xsl:text>",</xsl:text>
    </xsl:if>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>"schema":{</xsl:text>
    </xsl:if>
    <xsl:call-template name="type">
      <xsl:with-param name="type" select="@Type" />
      <xsl:with-param name="nullableFacet" select="@Nullable" />
      <xsl:with-param name="inParameter" select="true()" />
    </xsl:call-template>
    <xsl:if test="$openapi-version!='2.0'">
      <xsl:text>}</xsl:text>
    </xsl:if>
    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template match="edm:Parameter/@MaxLength">
    <xsl:if test=".!='max'">
      <xsl:text>,"maxLength":</xsl:text>
      <xsl:choose>
        <xsl:when test="$odata-version='2.0' and ../@Type='Edm.String'">
          <xsl:value-of select=".+2" />
        </xsl:when>
        <xsl:when test="$odata-version='2.0' and ../@Type='Edm.Binary'">
          <xsl:value-of select="2*.+3" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="." />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:if>
  </xsl:template>

  <xsl:template match="edm:Function/edm:Parameter" mode="path">
    <xsl:if test="position()>1">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:value-of select="@Name" />
    <xsl:text>=</xsl:text>
    <xsl:call-template name="pathValueSuffix">
      <xsl:with-param name="type" select="@Type" />
    </xsl:call-template>
    <xsl:text>{</xsl:text>
    <xsl:value-of select="@Name" />
    <xsl:text>}</xsl:text>
    <xsl:call-template name="pathValueSuffix">
      <xsl:with-param name="type" select="@Type" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="title-description">
    <xsl:param name="fallback-title" select="null" />
    <xsl:param name="suffix" select="null" />

    <xsl:variable name="title">
      <xsl:call-template name="Common.Label">
        <xsl:with-param name="node" select="." />
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test="$title!=''">
        <xsl:text>,"title":"</xsl:text>
        <xsl:value-of select="$title" />
        <xsl:value-of select="$suffix" />
        <xsl:text>"</xsl:text>
      </xsl:when>
      <xsl:when test="$fallback-title">
        <xsl:text>,"title":"</xsl:text>
        <xsl:value-of select="$fallback-title" />
        <xsl:value-of select="$suffix" />
        <xsl:text>"</xsl:text>
      </xsl:when>
    </xsl:choose>

    <xsl:variable name="description">
      <xsl:call-template name="description">
        <xsl:with-param name="node" select="." />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="$description!=''">
      <xsl:text>,"description":"</xsl:text>
      <xsl:value-of select="$description" />
      <xsl:text>"</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template name="summary-description-qualified">
    <xsl:param name="node" />
    <xsl:param name="qualifier" />
    <xsl:param name="fallback-summary" />

    <xsl:variable name="summary">
      <xsl:call-template name="Core.Description">
        <xsl:with-param name="node" select="$node" />
        <xsl:with-param name="qualifier" select="$qualifier" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:text>"summary":"</xsl:text>
    <xsl:choose>
      <xsl:when test="$summary!=''">
        <xsl:value-of select="$summary" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$fallback-summary" />
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>"</xsl:text>

    <xsl:variable name="description">
      <xsl:call-template name="Core.LongDescription">
        <xsl:with-param name="node" select="$node" />
        <xsl:with-param name="qualifier" select="$qualifier" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="$description!=''">
      <xsl:text>,"description":"</xsl:text>
      <xsl:value-of select="$description" />
      <xsl:text>"</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template name="summary-description">
    <xsl:param name="node" select="." />
    <xsl:param name="node2" select="." />
    <xsl:param name="fallback-summary" />

    <xsl:variable name="summary">
      <xsl:call-template name="Common.Label">
        <xsl:with-param name="node" select="$node" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:text>"summary":"</xsl:text>
    <xsl:choose>
      <xsl:when test="$summary!=''">
        <xsl:value-of select="$summary" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:variable name="summary2">
          <xsl:call-template name="Common.Label">
            <xsl:with-param name="node" select="$node2" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:choose>
          <xsl:when test="$summary2!=''">
            <xsl:value-of select="$summary2" />
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="$fallback-summary" />
          </xsl:otherwise>
        </xsl:choose>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>"</xsl:text>

    <xsl:variable name="description">
      <xsl:call-template name="description">
        <xsl:with-param name="node" select="$node" />
        <xsl:with-param name="node2" select="$node2" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="$description!=''">
      <xsl:text>,"description":"</xsl:text>
      <xsl:value-of select="$description" />
      <xsl:text>"</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template name="description">
    <xsl:param name="node" />
    <xsl:param name="node2" select="false()" />

    <xsl:variable name="quickinfo">
      <xsl:variable name="first">
        <xsl:call-template name="Common.QuickInfo">
          <xsl:with-param name="node" select="$node" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:value-of select="$first" />
      <xsl:if test="$first='' and $node2">
        <xsl:call-template name="Common.QuickInfo">
          <xsl:with-param name="node" select="$node2" />
        </xsl:call-template>
      </xsl:if>
    </xsl:variable>

    <xsl:variable name="description">
      <xsl:variable name="first">
        <xsl:call-template name="Core.Description">
          <xsl:with-param name="node" select="$node" />
        </xsl:call-template>
      </xsl:variable>
      <xsl:value-of select="$first" />
      <xsl:if test="$first='' and $node2">
        <xsl:call-template name="Core.Description">
          <xsl:with-param name="node" select="$node2" />
        </xsl:call-template>
      </xsl:if>
    </xsl:variable>

    <xsl:variable name="longdescription">
      <xsl:if test="$property-longDescription">
        <xsl:variable name="first">
          <xsl:call-template name="Core.LongDescription">
            <xsl:with-param name="node" select="$node" />
          </xsl:call-template>
        </xsl:variable>
        <xsl:value-of select="$first" />
        <xsl:if test="$first='' and $node2">
          <xsl:call-template name="Core.LongDescription">
            <xsl:with-param name="node" select="$node2" />
          </xsl:call-template>
        </xsl:if>
      </xsl:if>
    </xsl:variable>

    <xsl:choose>
      <xsl:when test="$quickinfo!='' or $description!='' or $longdescription!=''">
        <xsl:value-of select="$quickinfo" />
        <xsl:if test="$quickinfo!='' and $description!=''">
          <xsl:text>  \n</xsl:text>
        </xsl:if>
        <xsl:value-of select="$description" />
        <xsl:if test="($quickinfo!='' or $description!='') and $longdescription!=''">
          <xsl:text>  \n</xsl:text>
        </xsl:if>
        <xsl:value-of select="$longdescription" />
      </xsl:when>
      <!-- TODO: this is fishy, already used in summary -->
      <xsl:otherwise>
        <xsl:call-template name="Common.Label">
          <xsl:with-param name="node" select="$node" />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="escape">
    <xsl:param name="string" />
    <xsl:choose>
      <xsl:when test="contains($string,'&quot;')">
        <xsl:call-template name="replace">
          <xsl:with-param name="string" select="$string" />
          <xsl:with-param name="old" select="'&quot;'" />
          <xsl:with-param name="new" select="'\&quot;'" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="contains($string,'\')">
        <xsl:call-template name="replace">
          <xsl:with-param name="string" select="$string" />
          <xsl:with-param name="old" select="'\'" />
          <xsl:with-param name="new" select="'\\'" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="contains($string,'&#x0A;')">
        <xsl:call-template name="replace">
          <xsl:with-param name="string" select="$string" />
          <xsl:with-param name="old" select="'&#x0A;'" />
          <xsl:with-param name="new" select="'\n'" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="contains($string,'&#x0D;')">
        <xsl:call-template name="replace">
          <xsl:with-param name="string" select="$string" />
          <xsl:with-param name="old" select="'&#x0D;'" />
          <xsl:with-param name="new" select="''" />
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="contains($string,'&#x09;')">
        <xsl:call-template name="replace">
          <xsl:with-param name="string" select="$string" />
          <xsl:with-param name="old" select="'&#x09;'" />
          <xsl:with-param name="new" select="'\t'" />
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$string" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="replace">
    <xsl:param name="string" />
    <xsl:param name="old" />
    <xsl:param name="new" />
    <xsl:call-template name="escape">
      <xsl:with-param name="string" select="substring-before($string,$old)" />
    </xsl:call-template>
    <xsl:value-of select="$new" />
    <xsl:call-template name="escape">
      <xsl:with-param name="string" select="substring-after($string,$old)" />
    </xsl:call-template>
  </xsl:template>

  <!-- name : object -->
  <xsl:template match="@*|*" mode="object">
    <xsl:param name="name" />
    <xsl:param name="after" select="'something'" />
    <xsl:if test="position()=1">
      <xsl:if test="$after">
        <xsl:text>,</xsl:text>
      </xsl:if>
      <xsl:text>"</xsl:text>
      <xsl:value-of select="$name" />
      <xsl:text>":{</xsl:text>
    </xsl:if>
    <xsl:apply-templates select="." />
    <xsl:if test="position()!=last()">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:if test="position()=last()">
      <xsl:text>}</xsl:text>
    </xsl:if>
  </xsl:template>

  <!-- object within array -->
  <xsl:template match="*" mode="item">
    <xsl:text>{</xsl:text>
    <xsl:apply-templates select="@*|node()" mode="list" />
    <xsl:text>}</xsl:text>
  </xsl:template>

  <!-- name: hash -->
  <xsl:template match="*" mode="hash">
    <xsl:param name="name" />
    <xsl:param name="key" select="'Name'" />
    <xsl:param name="after" select="'something'" />
    <xsl:param name="constantProperties" />
    <xsl:param name="suffix" select="null" />
    <xsl:if test="position()=1">
      <xsl:if test="$after">
        <xsl:text>,</xsl:text>
      </xsl:if>
      <xsl:text>"</xsl:text>
      <xsl:value-of select="$name" />
      <xsl:text>":{</xsl:text>
    </xsl:if>
    <xsl:apply-templates select="." mode="hashpair">
      <xsl:with-param name="name" select="$name" />
      <xsl:with-param name="key" select="$key" />
      <xsl:with-param name="suffix" select="$suffix" />
    </xsl:apply-templates>
    <xsl:if test="position()!=last()">
      <xsl:text>,</xsl:text>
    </xsl:if>
    <xsl:if test="position()=last()">
      <xsl:value-of select="$constantProperties" />
      <xsl:text>}</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template match="*" mode="hashpair">
    <xsl:param name="name" />
    <xsl:param name="key" select="'Name'" />
    <xsl:param name="suffix" select="null" />
    <xsl:text>"</xsl:text>
    <xsl:value-of select="@*[local-name()=$key]" />
    <xsl:text>":{</xsl:text>
    <xsl:apply-templates select="." mode="hashvalue">
      <xsl:with-param name="name" select="$name" />
      <xsl:with-param name="key" select="$key" />
      <xsl:with-param name="suffix" select="$suffix" />
    </xsl:apply-templates>
    <xsl:text>}</xsl:text>
  </xsl:template>

  <xsl:template match="*" mode="hashvalue">
    <xsl:param name="key" select="'Name'" />
    <xsl:apply-templates select="@*[local-name()!=$key]|node()" mode="list" />
  </xsl:template>

  <!-- comma-separated list -->
  <xsl:template match="@*|*" mode="list">
    <xsl:param name="target" />
    <xsl:param name="qualifier" />
    <xsl:param name="after" />
    <xsl:choose>
      <xsl:when test="position() > 1">
        <xsl:text>,</xsl:text>
      </xsl:when>
      <xsl:when test="$after">
        <xsl:text>,</xsl:text>
      </xsl:when>
    </xsl:choose>
    <xsl:apply-templates select=".">
      <xsl:with-param name="target" select="$target" />
      <xsl:with-param name="qualifier" select="$qualifier" />
    </xsl:apply-templates>
  </xsl:template>

  <!-- continuation of comma-separated list -->
  <xsl:template match="@*|*" mode="list2">
    <xsl:param name="target" />
    <xsl:param name="qualifier" />
    <xsl:text>,</xsl:text>
    <xsl:apply-templates select=".">
      <xsl:with-param name="target" select="$target" />
      <xsl:with-param name="qualifier" select="$qualifier" />
    </xsl:apply-templates>
  </xsl:template>

  <!-- leftover attributes -->
  <xsl:template match="@*">
    <xsl:text>"TODO:@</xsl:text>
    <xsl:value-of select="local-name()" />
    <xsl:text>":"</xsl:text>
    <xsl:value-of select="." />
    <xsl:text>"</xsl:text>
  </xsl:template>

  <!-- leftover elements -->
  <xsl:template match="*">
    <xsl:text>"TODO:</xsl:text>
    <xsl:value-of select="local-name()" />
    <xsl:text>":{</xsl:text>
    <xsl:apply-templates select="@*|node()" mode="list" />
    <xsl:text>}</xsl:text>
  </xsl:template>

  <!-- leftover text -->
  <xsl:template match="text()">
    <xsl:text>"TODO:text()":"</xsl:text>
    <xsl:value-of select="." />
    <xsl:text>"</xsl:text>
  </xsl:template>

  <!-- helper functions -->
  <xsl:template name="substring-before-last">
    <xsl:param name="input" />
    <xsl:param name="marker" />
    <xsl:if test="contains($input,$marker)">
      <xsl:value-of select="substring-before($input,$marker)" />
      <xsl:if test="contains(substring-after($input,$marker),$marker)">
        <xsl:value-of select="$marker" />
        <xsl:call-template name="substring-before-last">
          <xsl:with-param name="input" select="substring-after($input,$marker)" />
          <xsl:with-param name="marker" select="$marker" />
        </xsl:call-template>
      </xsl:if>
    </xsl:if>
  </xsl:template>

  <xsl:template name="substring-after-last">
    <xsl:param name="input" />
    <xsl:param name="marker" />
    <xsl:choose>
      <xsl:when test="contains($input,$marker)">
        <xsl:call-template name="substring-after-last">
          <xsl:with-param name="input" select="substring-after($input,$marker)" />
          <xsl:with-param name="marker" select="$marker" />
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$input" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="replace-all">
    <xsl:param name="string" />
    <xsl:param name="old" />
    <xsl:param name="new" />
    <xsl:choose>
      <xsl:when test="contains($string,$old)">
        <xsl:value-of select="substring-before($string,$old)" />
        <xsl:value-of select="$new" />
        <xsl:call-template name="replace-all">
          <xsl:with-param name="string" select="substring-after($string,$old)" />
          <xsl:with-param name="old" select="$old" />
          <xsl:with-param name="new" select="$new" />
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$string" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="json-url">
    <xsl:param name="url" />
    <xsl:param name="root" select="''" />
    <xsl:variable name="jsonUrl">
      <xsl:choose>
        <xsl:when test="substring($url,string-length($url)-3) = '.xml'">
          <xsl:value-of select="substring($url,1,string-length($url)-4)" />
          <xsl:text>.openapi</xsl:text>
          <xsl:if test="$openapi-version!='2.0'">
            <xsl:text>3</xsl:text>
          </xsl:if>
          <xsl:text>.json</xsl:text>
        </xsl:when>
        <xsl:when test="string-length($url) = 0">
          <xsl:value-of select="$url" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$url" />
          <xsl:value-of select="$openapi-formatoption" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:choose>
      <!--
        <xsl:when test="substring($jsonUrl,1,1) = '/'">
        <xsl:value-of select="$scheme" />
        <xsl:text>://</xsl:text>
        <xsl:value-of select="$host" />
        <xsl:value-of select="$jsonUrl" />
        </xsl:when>
        <xsl:when test="substring($jsonUrl,1,3) = '../'">
        <xsl:value-of select="$scheme" />
        <xsl:text>://</xsl:text>
        <xsl:value-of select="$host" />
        <xsl:value-of select="$basePath" />
        <xsl:text>/</xsl:text>
        <xsl:value-of select="$jsonUrl" />
        </xsl:when>
      -->
      <xsl:when test="substring($jsonUrl,1,2) = './' and $root!=''">
        <xsl:value-of select="$root" />
        <xsl:value-of select="substring($jsonUrl,3)" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$jsonUrl" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="normalizedQualifiedName">
    <xsl:param name="qualifiedName" />
    <xsl:variable name="qualifier">
      <xsl:call-template name="substring-before-last">
        <xsl:with-param name="input" select="$qualifiedName" />
        <xsl:with-param name="marker" select="'.'" />
      </xsl:call-template>
    </xsl:variable>
    <xsl:choose>
      <xsl:when test="//edm:Schema[@Namespace=$qualifier and @Alias]">
        <xsl:value-of select="//edm:Schema[@Namespace=$qualifier]/@Alias" />
      </xsl:when>
      <xsl:when test="//edmx:Include[@Namespace=$qualifier and @Alias]">
        <xsl:value-of select="//edmx:Include[@Namespace=$qualifier]/@Alias" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$qualifier" />
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text>.</xsl:text>
    <xsl:call-template name="substring-after-last">
      <xsl:with-param name="input" select="$qualifiedName" />
      <xsl:with-param name="marker" select="'.'" />
    </xsl:call-template>
  </xsl:template>

</xsl:stylesheet>