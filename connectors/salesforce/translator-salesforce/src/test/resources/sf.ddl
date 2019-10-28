CREATE FOREIGN TABLE Account (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterRecordId string(18) OPTIONS (NAMEINSOURCE 'MasterRecordId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Prospect,Customer - Direct,Customer - Channel,Channel Partner / Reseller,Installation Partner,Technology Partner,Other'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingStreet string(255) OPTIONS (NAMEINSOURCE 'BillingStreet', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingCity string(40) OPTIONS (NAMEINSOURCE 'BillingCity', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingState string(80) OPTIONS (NAMEINSOURCE 'BillingState', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingPostalCode string(20) OPTIONS (NAMEINSOURCE 'BillingPostalCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingCountry string(80) OPTIONS (NAMEINSOURCE 'BillingCountry', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ShippingStreet string(255) OPTIONS (NAMEINSOURCE 'ShippingStreet', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ShippingCity string(40) OPTIONS (NAMEINSOURCE 'ShippingCity', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ShippingState string(80) OPTIONS (NAMEINSOURCE 'ShippingState', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ShippingPostalCode string(20) OPTIONS (NAMEINSOURCE 'ShippingPostalCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ShippingCountry string(80) OPTIONS (NAMEINSOURCE 'ShippingCountry', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Phone string(40) OPTIONS (NAMEINSOURCE 'Phone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Fax string(40) OPTIONS (NAMEINSOURCE 'Fax', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountNumber string(40) OPTIONS (NAMEINSOURCE 'AccountNumber', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Website string(255) OPTIONS (NAMEINSOURCE 'Website', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Sic string(20) OPTIONS (NAMEINSOURCE 'Sic', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Industry string(40) OPTIONS (NAMEINSOURCE 'Industry', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Agriculture,Apparel,Banking,Biotechnology,Chemicals,Communications,Construction,Consulting,Education,Electronics,Energy,Engineering,Entertainment,Environmental,Finance,Food & Beverage,Government,Healthcare,Hospitality,Insurance,Machinery,Manufacturing,Media,Not For Profit,Recreation,Retail,Shipping,Technology,Telecommunications,Transportation,Utilities,Other'),
    AnnualRevenue double OPTIONS (NAMEINSOURCE 'AnnualRevenue', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfEmployees integer OPTIONS (NAMEINSOURCE 'NumberOfEmployees', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Ownership string(40) OPTIONS (NAMEINSOURCE 'Ownership', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Public,Private,Subsidiary,Other'),
    TickerSymbol string(20) OPTIONS (NAMEINSOURCE 'TickerSymbol', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Rating string(40) OPTIONS (NAMEINSOURCE 'Rating', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Hot,Warm,Cold'),
    Site string(80) OPTIONS (NAMEINSOURCE 'Site', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LastActivityDate date OPTIONS (NAMEINSOURCE 'LastActivityDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Jigsaw string(20) OPTIONS (NAMEINSOURCE 'Jigsaw', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CustomerPriority__c string(255) OPTIONS (NAMEINSOURCE 'CustomerPriority__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'High,Low,Medium'),
    SLA__c string(255) OPTIONS (NAMEINSOURCE 'SLA__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Gold,Silver,Platinum,Bronze'),
    Active__c string(255) OPTIONS (NAMEINSOURCE 'Active__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'No,Yes'),
    NumberofLocations__c double OPTIONS (NAMEINSOURCE 'NumberofLocations__c', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    UpsellOpportunity__c string(255) OPTIONS (NAMEINSOURCE 'UpsellOpportunity__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Maybe,No,Yes'),
    SLASerialNumber__c string(10) OPTIONS (NAMEINSOURCE 'SLASerialNumber__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    SLAExpirationDate__c date OPTIONS (NAMEINSOURCE 'SLAExpirationDate__c', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_ParentId FOREIGN KEY(ParentId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'ChildAccounts')
) OPTIONS (NAMEINSOURCE 'Account', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'true', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE AccountContactRole (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    "Role" string(40) OPTIONS (NAMEINSOURCE 'Role', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Business User,Decision Maker,Economic Buyer,Economic Decision Maker,Evaluator,Executive Sponsor,Influencer,Technical Buyer,Other'),
    IsPrimary boolean OPTIONS (NAMEINSOURCE 'IsPrimary', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'AccountContactRoles'),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'AccountContactRoles')
) OPTIONS (NAMEINSOURCE 'AccountContactRole', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE AccountFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_ParentId FOREIGN KEY(ParentId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'AccountFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE AccountHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'accountMerged,AccountNumber,AccountSource,Active__c,AnnualRevenue,BillingAddress,BillingCity,BillingCountry,BillingLatitude,BillingLongitude,BillingPostalCode,BillingState,BillingStreet,created,accountCreatedFromLead,CustomerPriority__c,Description,Fax,feedEvent,Industry,Jigsaw,accountUpdatedByLead,personAccountUpdatedByLead,Name,NumberOfEmployees,NumberofLocations__c,Owner,ownerAccepted,ownerAssignment,Ownership,Parent,Phone,Rating,locked,unlocked,ShippingAddress,ShippingCity,ShippingCountry,ShippingLatitude,ShippingLongitude,ShippingPostalCode,ShippingState,ShippingStreet,Sic,SicDesc,Site,SLA__c,SLAExpirationDate__c,SLASerialNumber__c,TextName,TickerSymbol,Type,UpsellOpportunity__c,Website'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'AccountHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE AccountPartner (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountFromId string(18) OPTIONS (NAMEINSOURCE 'AccountFromId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountToId string(18) OPTIONS (NAMEINSOURCE 'AccountToId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    "Role" string(40) OPTIONS (NAMEINSOURCE 'Role', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'System Integrator,Agency,Advertiser,VAR/Reseller,Distributor,Developer,Broker,Lender,Supplier,Institution,Contractor,Dealer,Consultant'),
    IsPrimary boolean OPTIONS (NAMEINSOURCE 'IsPrimary', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ReversePartnerId string(18) OPTIONS (NAMEINSOURCE 'ReversePartnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountFromId FOREIGN KEY(AccountFromId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'AccountPartnersFrom'),
    CONSTRAINT FK_Account_AccountToId FOREIGN KEY(AccountToId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'AccountPartnersTo'),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'AccountPartners')
) OPTIONS (NAMEINSOURCE 'AccountPartner', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE AccountShare (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserOrGroupId string(18) OPTIONS (NAMEINSOURCE 'UserOrGroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountAccessLevel string(40) OPTIONS (NAMEINSOURCE 'AccountAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Read,Edit,All'),
    OpportunityAccessLevel string(40) OPTIONS (NAMEINSOURCE 'OpportunityAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    CaseAccessLevel string(40) OPTIONS (NAMEINSOURCE 'CaseAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    ContactAccessLevel string(40) OPTIONS (NAMEINSOURCE 'ContactAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    RowCause string(40) OPTIONS (NAMEINSOURCE 'RowCause', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Owner,Manual,Rule,ImplicitChild,ImplicitParent,Team,Territory,TerritoryManual,TerritoryRule'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Shares')
) OPTIONS (NAMEINSOURCE 'AccountShare', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ActivityHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WhoId string(18) OPTIONS (NAMEINSOURCE 'WhoId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WhatId string(18) OPTIONS (NAMEINSOURCE 'WhatId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Subject string(80) OPTIONS (NAMEINSOURCE 'Subject', NATIVE_TYPE 'combobox', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsTask boolean OPTIONS (NAMEINSOURCE 'IsTask', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ActivityDate date OPTIONS (NAMEINSOURCE 'ActivityDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Not Started,In Progress,Completed,Waiting on someone else,Deferred'),
    Priority string(40) OPTIONS (NAMEINSOURCE 'Priority', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'High,Normal,Low'),
    ActivityType string(40) OPTIONS (NAMEINSOURCE 'ActivityType', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Call,Call,Email,Email,Meeting,Meeting,Other,Other'),
    IsClosed boolean OPTIONS (NAMEINSOURCE 'IsClosed', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsAllDayEvent boolean OPTIONS (NAMEINSOURCE 'IsAllDayEvent', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsVisibleInSelfService boolean OPTIONS (NAMEINSOURCE 'IsVisibleInSelfService', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    DurationInMinutes integer OPTIONS (NAMEINSOURCE 'DurationInMinutes', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Location string(80) OPTIONS (NAMEINSOURCE 'Location', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CallDurationInSeconds integer OPTIONS (NAMEINSOURCE 'CallDurationInSeconds', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CallType string(40) OPTIONS (NAMEINSOURCE 'CallType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Internal,Inbound,Outbound'),
    CallDisposition string(255) OPTIONS (NAMEINSOURCE 'CallDisposition', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CallObject string(255) OPTIONS (NAMEINSOURCE 'CallObject', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ReminderDateTime timestamp OPTIONS (NAMEINSOURCE 'ReminderDateTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsReminderSet boolean OPTIONS (NAMEINSOURCE 'IsReminderSet', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Asset_WhatId FOREIGN KEY(WhatId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Campaign_WhatId FOREIGN KEY(WhatId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Case__WhatId FOREIGN KEY(WhatId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Contact_WhoId FOREIGN KEY(WhoId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Contract_WhatId FOREIGN KEY(WhatId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Lead_WhoId FOREIGN KEY(WhoId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Opportunity_WhatId FOREIGN KEY(WhatId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Product2_WhatId FOREIGN KEY(WhatId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories'),
    CONSTRAINT FK_Solution_WhatId FOREIGN KEY(WhatId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'ActivityHistories')
) OPTIONS (NAMEINSOURCE 'ActivityHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE AdditionalNumber (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CallCenterId string(18) OPTIONS (NAMEINSOURCE 'CallCenterId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Phone string(40) OPTIONS (NAMEINSOURCE 'Phone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'AdditionalNumber', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE AggregateResult (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'AggregateResult', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ApexClass (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApiVersion double OPTIONS (NAMEINSOURCE 'ApiVersion', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Inactive,Active,Deleted'),
    IsValid boolean OPTIONS (NAMEINSOURCE 'IsValid', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    BodyCrc double OPTIONS (NAMEINSOURCE 'BodyCrc', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Body string(1000000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LengthWithoutComments integer OPTIONS (NAMEINSOURCE 'LengthWithoutComments', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ApexClass', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE ApexComponent (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApiVersion double OPTIONS (NAMEINSOURCE 'ApiVersion', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MasterLabel string(80) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ControllerType string(40) OPTIONS (NAMEINSOURCE 'ControllerType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,4,2,3,5'),
    ControllerKey string(255) OPTIONS (NAMEINSOURCE 'ControllerKey', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Markup string(1048576) OPTIONS (NAMEINSOURCE 'Markup', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ApexComponent', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE ApexLog (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LogUserId string(18) OPTIONS (NAMEINSOURCE 'LogUserId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LogLength integer OPTIONS (NAMEINSOURCE 'LogLength', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Request string(16) OPTIONS (NAMEINSOURCE 'Request', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Operation string(128) OPTIONS (NAMEINSOURCE 'Operation', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Application string(64) OPTIONS (NAMEINSOURCE 'Application', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(255) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DurationMilliseconds integer OPTIONS (NAMEINSOURCE 'DurationMilliseconds', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StartTime timestamp OPTIONS (NAMEINSOURCE 'StartTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Location string(40) OPTIONS (NAMEINSOURCE 'Location', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Monitoring,SystemLog,HeapDump,Preserved'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ApexLog', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ApexPage (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApiVersion double OPTIONS (NAMEINSOURCE 'ApiVersion', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MasterLabel string(80) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ControllerType string(40) OPTIONS (NAMEINSOURCE 'ControllerType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,4,2,3,5'),
    ControllerKey string(255) OPTIONS (NAMEINSOURCE 'ControllerKey', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Markup string(1048576) OPTIONS (NAMEINSOURCE 'Markup', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ApexPage', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE ApexTrigger (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TableEnumOrId string(40) OPTIONS (NAMEINSOURCE 'TableEnumOrId', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,Asset,Attachment,Campaign,CampaignMember,Case,CaseComment,CollaborationFolder,CollaborationFolderMember,CollaborationGroup,CollaborationGroupMember,Contact,ContentDocument,ContentVersion,Contract,Event,FeedComment,FeedItem,Idea,IdeaComment,Lead,Note,Opportunity,OpportunityLineItem,Partner,Pricebook2,Product2,SocialPersona,Solution,Task,Topic,TopicAssignment,User'),
    UsageBeforeInsert boolean OPTIONS (NAMEINSOURCE 'UsageBeforeInsert', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsageAfterInsert boolean OPTIONS (NAMEINSOURCE 'UsageAfterInsert', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsageBeforeUpdate boolean OPTIONS (NAMEINSOURCE 'UsageBeforeUpdate', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsageAfterUpdate boolean OPTIONS (NAMEINSOURCE 'UsageAfterUpdate', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsageBeforeDelete boolean OPTIONS (NAMEINSOURCE 'UsageBeforeDelete', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsageAfterDelete boolean OPTIONS (NAMEINSOURCE 'UsageAfterDelete', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsageIsBulk boolean OPTIONS (NAMEINSOURCE 'UsageIsBulk', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsageAfterUndelete boolean OPTIONS (NAMEINSOURCE 'UsageAfterUndelete', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApiVersion double OPTIONS (NAMEINSOURCE 'ApiVersion', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Inactive,Active,Deleted'),
    IsValid boolean OPTIONS (NAMEINSOURCE 'IsValid', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    BodyCrc double OPTIONS (NAMEINSOURCE 'BodyCrc', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Body string(1000000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LengthWithoutComments integer OPTIONS (NAMEINSOURCE 'LengthWithoutComments', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ApexTrigger', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE Approval (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Pending,Approved,Rejected'),
    RequestComment string OPTIONS (NAMEINSOURCE 'RequestComment', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApproveComment string OPTIONS (NAMEINSOURCE 'ApproveComment', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contract_ParentId FOREIGN KEY(ParentId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'Approvals')
) OPTIONS (NAMEINSOURCE 'Approval', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Asset (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Product2Id string(18) OPTIONS (NAMEINSOURCE 'Product2Id', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsCompetitorProduct boolean OPTIONS (NAMEINSOURCE 'IsCompetitorProduct', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SerialNumber string(80) OPTIONS (NAMEINSOURCE 'SerialNumber', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InstallDate date OPTIONS (NAMEINSOURCE 'InstallDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PurchaseDate date OPTIONS (NAMEINSOURCE 'PurchaseDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsageEndDate date OPTIONS (NAMEINSOURCE 'UsageEndDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Shipped,Installed,Registered,Obsolete,Purchased'),
    Price double OPTIONS (NAMEINSOURCE 'Price', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Quantity double OPTIONS (NAMEINSOURCE 'Quantity', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Assets'),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Assets'),
    CONSTRAINT FK_Product2_Product2Id FOREIGN KEY(Product2Id) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'Assets')
) OPTIONS (NAMEINSOURCE 'Asset', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE AssetFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Asset_ParentId FOREIGN KEY(ParentId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'AssetFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE AssignmentRule (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(120) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SobjectType string(40) OPTIONS (NAMEINSOURCE 'SobjectType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Case,Lead'),
    Active boolean OPTIONS (NAMEINSOURCE 'Active', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'AssignmentRule', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE AsyncApexJob (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    JobType string(40) OPTIONS (NAMEINSOURCE 'JobType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Future,SharingRecalculation,ScheduledApex,BatchApex,BatchApexWorker,TestRequest,TestWorker,ApexToken'),
    ApexClassId string(18) OPTIONS (NAMEINSOURCE 'ApexClassId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Queued,Processing,Aborted,Completed,Failed,Preparing,Holding'),
    JobItemsProcessed integer OPTIONS (NAMEINSOURCE 'JobItemsProcessed', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TotalJobItems integer OPTIONS (NAMEINSOURCE 'TotalJobItems', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfErrors integer OPTIONS (NAMEINSOURCE 'NumberOfErrors', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CompletedDate timestamp OPTIONS (NAMEINSOURCE 'CompletedDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MethodName string(255) OPTIONS (NAMEINSOURCE 'MethodName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ExtendedStatus string(255) OPTIONS (NAMEINSOURCE 'ExtendedStatus', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ParentJobId string(18) OPTIONS (NAMEINSOURCE 'ParentJobId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastProcessed string(15) OPTIONS (NAMEINSOURCE 'LastProcessed', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastProcessedOffset integer OPTIONS (NAMEINSOURCE 'LastProcessedOffset', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'AsyncApexJob', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Attachment (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsPrivate boolean OPTIONS (NAMEINSOURCE 'IsPrivate', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BodyLength integer OPTIONS (NAMEINSOURCE 'BodyLength', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body blob OPTIONS (NAMEINSOURCE 'Body', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Description string(500) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_ParentId FOREIGN KEY(ParentId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Asset_ParentId FOREIGN KEY(ParentId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Campaign_ParentId FOREIGN KEY(ParentId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Case__ParentId FOREIGN KEY(ParentId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Contact_ParentId FOREIGN KEY(ParentId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Contract_ParentId FOREIGN KEY(ParentId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_EmailTemplate_ParentId FOREIGN KEY(ParentId) REFERENCES EmailTemplate (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Event_ParentId FOREIGN KEY(ParentId) REFERENCES Event (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Lead_ParentId FOREIGN KEY(ParentId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Opportunity_ParentId FOREIGN KEY(ParentId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Product2_ParentId FOREIGN KEY(ParentId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Solution_ParentId FOREIGN KEY(ParentId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'Attachments'),
    CONSTRAINT FK_Task_ParentId FOREIGN KEY(ParentId) REFERENCES Task (Id) OPTIONS (NAMEINSOURCE 'Attachments')
) OPTIONS (NAMEINSOURCE 'Attachment', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE BrandTemplate (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Description string(1000) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Value_ string(32000) OPTIONS (NAMEINSOURCE 'Value', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'BrandTemplate', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE BusinessHours (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDefault boolean OPTIONS (NAMEINSOURCE 'IsDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    SundayStartTime time OPTIONS (NAMEINSOURCE 'SundayStartTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SundayEndTime time OPTIONS (NAMEINSOURCE 'SundayEndTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MondayStartTime time OPTIONS (NAMEINSOURCE 'MondayStartTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MondayEndTime time OPTIONS (NAMEINSOURCE 'MondayEndTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TuesdayStartTime time OPTIONS (NAMEINSOURCE 'TuesdayStartTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TuesdayEndTime time OPTIONS (NAMEINSOURCE 'TuesdayEndTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WednesdayStartTime time OPTIONS (NAMEINSOURCE 'WednesdayStartTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WednesdayEndTime time OPTIONS (NAMEINSOURCE 'WednesdayEndTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ThursdayStartTime time OPTIONS (NAMEINSOURCE 'ThursdayStartTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ThursdayEndTime time OPTIONS (NAMEINSOURCE 'ThursdayEndTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FridayStartTime time OPTIONS (NAMEINSOURCE 'FridayStartTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FridayEndTime time OPTIONS (NAMEINSOURCE 'FridayEndTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SaturdayStartTime time OPTIONS (NAMEINSOURCE 'SaturdayStartTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SaturdayEndTime time OPTIONS (NAMEINSOURCE 'SaturdayEndTime', NATIVE_TYPE 'time', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TimeZoneSidKey string(40) OPTIONS (NAMEINSOURCE 'TimeZoneSidKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Pacific/Kiritimati,Pacific/Enderbury,Pacific/Tongatapu,Pacific/Chatham,Asia/Kamchatka,Pacific/Auckland,Pacific/Fiji,Pacific/Norfolk,Pacific/Guadalcanal,Australia/Lord_Howe,Australia/Brisbane,Australia/Sydney,Australia/Adelaide,Australia/Darwin,Asia/Seoul,Asia/Tokyo,Asia/Hong_Kong,Asia/Kuala_Lumpur,Asia/Manila,Asia/Shanghai,Asia/Singapore,Asia/Taipei,Australia/Perth,Asia/Bangkok,Asia/Ho_Chi_Minh,Asia/Jakarta,Asia/Rangoon,Asia/Dhaka,Asia/Yekaterinburg,Asia/Kathmandu,Asia/Colombo,Asia/Kolkata,Asia/Karachi,Asia/Tashkent,Asia/Kabul,Asia/Tehran,Asia/Dubai,Asia/Tbilisi,Europe/Moscow,Africa/Nairobi,Asia/Baghdad,Asia/Jerusalem,Asia/Kuwait,Asia/Riyadh,Europe/Athens,Europe/Bucharest,Europe/Helsinki,Europe/Istanbul,Europe/Minsk,Africa/Cairo,Africa/Johannesburg,Europe/Amsterdam,Europe/Berlin,Europe/Brussels,Europe/Paris,Europe/Prague,Europe/Rome,Africa/Algiers,Europe/Dublin,Europe/Lisbon,Europe/London,GMT,Atlantic/Cape_Verde,Atlantic/South_Georgia,America/St_Johns,America/Argentina/Buenos_Aires,America/Halifax,America/Sao_Paulo,Atlantic/Bermuda,America/Indiana/Indianapolis,America/New_York,America/Puerto_Rico,America/Santiago,America/Caracas,America/Bogota,America/Chicago,America/Lima,America/Mexico_City,America/Panama,America/Denver,America/El_Salvador,America/Los_Angeles,America/Phoenix,America/Tijuana,America/Anchorage,Pacific/Honolulu,Pacific/Niue,Pacific/Pago_Pago'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'BusinessHours', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE BusinessProcess (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TableEnumOrId string(40) OPTIONS (NAMEINSOURCE 'TableEnumOrId', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Case,Lead,Opportunity,Solution'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'BusinessProcess', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CallCenter (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InternalName string(240) OPTIONS (NAMEINSOURCE 'InternalName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Version double OPTIONS (NAMEINSOURCE 'Version', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CallCenter', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Campaign (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Conference,Webinar,Trade Show,Public Relations,Partners,Referral Program,Advertisement,Banner Ads,Direct Mail,Email,Telemarketing,Other'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Planned,In Progress,Completed,Aborted'),
    StartDate date OPTIONS (NAMEINSOURCE 'StartDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EndDate date OPTIONS (NAMEINSOURCE 'EndDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ExpectedRevenue double OPTIONS (NAMEINSOURCE 'ExpectedRevenue', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BudgetedCost double OPTIONS (NAMEINSOURCE 'BudgetedCost', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ActualCost double OPTIONS (NAMEINSOURCE 'ActualCost', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ExpectedResponse double OPTIONS (NAMEINSOURCE 'ExpectedResponse', NATIVE_TYPE 'percent', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberSent double OPTIONS (NAMEINSOURCE 'NumberSent', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfLeads integer OPTIONS (NAMEINSOURCE 'NumberOfLeads', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfConvertedLeads integer OPTIONS (NAMEINSOURCE 'NumberOfConvertedLeads', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfContacts integer OPTIONS (NAMEINSOURCE 'NumberOfContacts', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfResponses integer OPTIONS (NAMEINSOURCE 'NumberOfResponses', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfOpportunities integer OPTIONS (NAMEINSOURCE 'NumberOfOpportunities', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfWonOpportunities integer OPTIONS (NAMEINSOURCE 'NumberOfWonOpportunities', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AmountAllOpportunities double OPTIONS (NAMEINSOURCE 'AmountAllOpportunities', UPDATABLE FALSE, CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AmountWonOpportunities double OPTIONS (NAMEINSOURCE 'AmountWonOpportunities', UPDATABLE FALSE, CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LastActivityDate date OPTIONS (NAMEINSOURCE 'LastActivityDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CampaignMemberRecordTypeId string(18) OPTIONS (NAMEINSOURCE 'CampaignMemberRecordTypeId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Campaign_ParentId FOREIGN KEY(ParentId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'ChildCampaigns')
) OPTIONS (NAMEINSOURCE 'Campaign', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE CampaignFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Campaign_ParentId FOREIGN KEY(ParentId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'CampaignFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CampaignMember (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CampaignId string(18) OPTIONS (NAMEINSOURCE 'CampaignId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LeadId string(18) OPTIONS (NAMEINSOURCE 'LeadId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Planned,Received,Responded,Sent'),
    HasResponded boolean OPTIONS (NAMEINSOURCE 'HasResponded', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FirstRespondedDate date OPTIONS (NAMEINSOURCE 'FirstRespondedDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Campaign_CampaignId FOREIGN KEY(CampaignId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'CampaignMembers'),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'CampaignMembers'),
    CONSTRAINT FK_Lead_LeadId FOREIGN KEY(LeadId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'CampaignMembers')
) OPTIONS (NAMEINSOURCE 'CampaignMember', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CampaignMemberStatus (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CampaignId string(18) OPTIONS (NAMEINSOURCE 'CampaignId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Label string(765) OPTIONS (NAMEINSOURCE 'Label', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDefault boolean OPTIONS (NAMEINSOURCE 'IsDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    HasResponded boolean OPTIONS (NAMEINSOURCE 'HasResponded', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CampaignMemberStatus', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CampaignShare (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CampaignId string(18) OPTIONS (NAMEINSOURCE 'CampaignId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserOrGroupId string(18) OPTIONS (NAMEINSOURCE 'UserOrGroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CampaignAccessLevel string(40) OPTIONS (NAMEINSOURCE 'CampaignAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Read,Edit,All'),
    RowCause string(40) OPTIONS (NAMEINSOURCE 'RowCause', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Owner,Manual,Rule,ImplicitChild,ImplicitParent,Team,Territory,TerritoryManual,TerritoryRule'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Campaign_CampaignId FOREIGN KEY(CampaignId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'Shares')
) OPTIONS (NAMEINSOURCE 'CampaignShare', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Case_ (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CaseNumber string(30) OPTIONS (NAMEINSOURCE 'CaseNumber', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AssetId string(18) OPTIONS (NAMEINSOURCE 'AssetId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SuppliedName string(80) OPTIONS (NAMEINSOURCE 'SuppliedName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SuppliedEmail string(80) OPTIONS (NAMEINSOURCE 'SuppliedEmail', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SuppliedPhone string(40) OPTIONS (NAMEINSOURCE 'SuppliedPhone', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SuppliedCompany string(80) OPTIONS (NAMEINSOURCE 'SuppliedCompany', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Mechanical,Electrical,Electronic,Structural,Other'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true', "teiid_sf:Picklist Values" 'New,Working,Escalated,Closed'),
    Reason string(40) OPTIONS (NAMEINSOURCE 'Reason', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Installation,Equipment Complexity,Performance,Breakdown,Equipment Design,Feedback,Other'),
    Origin string(40) OPTIONS (NAMEINSOURCE 'Origin', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Phone,Email,Web'),
    Subject string(255) OPTIONS (NAMEINSOURCE 'Subject', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Priority string(40) OPTIONS (NAMEINSOURCE 'Priority', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'High,Medium,Low'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsClosed boolean OPTIONS (NAMEINSOURCE 'IsClosed', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ClosedDate timestamp OPTIONS (NAMEINSOURCE 'ClosedDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsEscalated boolean OPTIONS (NAMEINSOURCE 'IsEscalated', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    EngineeringReqNumber__c string(12) OPTIONS (NAMEINSOURCE 'EngineeringReqNumber__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    SLAViolation__c string(255) OPTIONS (NAMEINSOURCE 'SLAViolation__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'No,Yes'),
    Product__c string(255) OPTIONS (NAMEINSOURCE 'Product__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'GC1040,GC1060,GC3020,GC3040,GC3060,GC5020,GC5040,GC5060,GC1020'),
    PotentialLiability__c string(255) OPTIONS (NAMEINSOURCE 'PotentialLiability__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'No,Yes'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Cases'),
    CONSTRAINT FK_Asset_AssetId FOREIGN KEY(AssetId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'Cases'),
    CONSTRAINT FK_Case__ParentId FOREIGN KEY(ParentId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'Cases'),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Cases')
) OPTIONS (NAMEINSOURCE 'Case', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE CaseComment (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsPublished boolean OPTIONS (NAMEINSOURCE 'IsPublished', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentBody string OPTIONS (NAMEINSOURCE 'CommentBody', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Case__ParentId FOREIGN KEY(ParentId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'CaseComments')
) OPTIONS (NAMEINSOURCE 'CaseComment', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE CaseContactRole (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CasesId string(18) OPTIONS (NAMEINSOURCE 'CasesId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    "Role" string(40) OPTIONS (NAMEINSOURCE 'Role', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Technical Contact,Business Contact,Decision Maker,Other'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Case__CasesId FOREIGN KEY(CasesId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'CaseContactRoles'),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'CaseContactRoles')
) OPTIONS (NAMEINSOURCE 'CaseContactRole', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Case__ParentId FOREIGN KEY(ParentId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'CaseFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CaseId string(18) OPTIONS (NAMEINSOURCE 'CaseId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,Asset,BusinessHours,closed,Contact,created,Description,EngineeringReqNumber__c,feedEvent,IsClosedOnCreate,IsEscalated,Origin,Owner,ownerAccepted,ownerAssignment,ownerEscalated,Parent,PotentialLiability__c,Priority,Product__c,Reason,locked,unlocked,SLAViolation__c,Status,Subject,Type'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Case__CaseId FOREIGN KEY(CaseId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'CaseHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseShare (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CaseId string(18) OPTIONS (NAMEINSOURCE 'CaseId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserOrGroupId string(18) OPTIONS (NAMEINSOURCE 'UserOrGroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CaseAccessLevel string(40) OPTIONS (NAMEINSOURCE 'CaseAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Read,Edit,All'),
    RowCause string(40) OPTIONS (NAMEINSOURCE 'RowCause', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Owner,Manual,Rule,ImplicitChild,ImplicitParent,Team,Territory,TerritoryManual,TerritoryRule'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Case__CaseId FOREIGN KEY(CaseId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'Shares')
) OPTIONS (NAMEINSOURCE 'CaseShare', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseSolution (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CaseId string(18) OPTIONS (NAMEINSOURCE 'CaseId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SolutionId string(18) OPTIONS (NAMEINSOURCE 'SolutionId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Case__CaseId FOREIGN KEY(CaseId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'CaseSolutions'),
    CONSTRAINT FK_Solution_SolutionId FOREIGN KEY(SolutionId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'CaseSolutions')
) OPTIONS (NAMEINSOURCE 'CaseSolution', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseStatus (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDefault boolean OPTIONS (NAMEINSOURCE 'IsDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsClosed boolean OPTIONS (NAMEINSOURCE 'IsClosed', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CaseStatus', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseTeamMember (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MemberId string(18) OPTIONS (NAMEINSOURCE 'MemberId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TeamTemplateMemberId string(18) OPTIONS (NAMEINSOURCE 'TeamTemplateMemberId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TeamRoleId string(18) OPTIONS (NAMEINSOURCE 'TeamRoleId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Case__ParentId FOREIGN KEY(ParentId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'TeamMembers')
) OPTIONS (NAMEINSOURCE 'CaseTeamMember', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseTeamRole (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccessLevel string(40) OPTIONS (NAMEINSOURCE 'AccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    PreferencesVisibleInCSP boolean OPTIONS (NAMEINSOURCE 'PreferencesVisibleInCSP', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CaseTeamRole', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseTeamTemplate (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(300) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CaseTeamTemplate', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseTeamTemplateMember (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    TeamTemplateId string(18) OPTIONS (NAMEINSOURCE 'TeamTemplateId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MemberId string(18) OPTIONS (NAMEINSOURCE 'MemberId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TeamRoleId string(18) OPTIONS (NAMEINSOURCE 'TeamRoleId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CaseTeamTemplateMember', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CaseTeamTemplateRecord (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TeamTemplateId string(18) OPTIONS (NAMEINSOURCE 'TeamTemplateId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Case__ParentId FOREIGN KEY(ParentId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'TeamTemplateRecords')
) OPTIONS (NAMEINSOURCE 'CaseTeamTemplateRecord', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CategoryData (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CategoryNodeId string(18) OPTIONS (NAMEINSOURCE 'CategoryNodeId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedSobjectId string(18) OPTIONS (NAMEINSOURCE 'RelatedSobjectId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CategoryData', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CategoryNode (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MasterLabel string(40) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortStyle string(40) OPTIONS (NAMEINSOURCE 'SortStyle', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'custom,alphabetical'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CategoryNode', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ClientBrowser (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    UsersId string(18) OPTIONS (NAMEINSOURCE 'UsersId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FullUserAgent string(1024) OPTIONS (NAMEINSOURCE 'FullUserAgent', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ProxyInfo string(1024) OPTIONS (NAMEINSOURCE 'ProxyInfo', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastUpdate timestamp OPTIONS (NAMEINSOURCE 'LastUpdate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ClientBrowser', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CollaborationGroup (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(40) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MemberCount integer OPTIONS (NAMEINSOURCE 'MemberCount', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CollaborationType string(40) OPTIONS (NAMEINSOURCE 'CollaborationType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Public,Private'),
    Description string(1000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FullPhotoUrl string(1024) OPTIONS (NAMEINSOURCE 'FullPhotoUrl', UPDATABLE FALSE, NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SmallPhotoUrl string(1024) OPTIONS (NAMEINSOURCE 'SmallPhotoUrl', UPDATABLE FALSE, NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastFeedModifiedDate timestamp OPTIONS (NAMEINSOURCE 'LastFeedModifiedDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InformationTitle string(30) OPTIONS (NAMEINSOURCE 'InformationTitle', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InformationBody string(1000) OPTIONS (NAMEINSOURCE 'InformationBody', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    HasPrivateFieldsAccess boolean OPTIONS (NAMEINSOURCE 'HasPrivateFieldsAccess', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CollaborationGroup', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE CollaborationGroupFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_CollaborationGroup_ParentId FOREIGN KEY(ParentId) REFERENCES CollaborationGroup (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'CollaborationGroupFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CollaborationGroupMember (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CollaborationGroupId string(18) OPTIONS (NAMEINSOURCE 'CollaborationGroupId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MemberId string(18) OPTIONS (NAMEINSOURCE 'MemberId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NotificationFrequency string(40) OPTIONS (NAMEINSOURCE 'NotificationFrequency', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true', "teiid_sf:Picklist Values" 'P,D,W,N'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_CollaborationGroup_CollaborationGroupId FOREIGN KEY(CollaborationGroupId) REFERENCES CollaborationGroup (Id) OPTIONS (NAMEINSOURCE 'GroupMembers'),
    CONSTRAINT FK_User__MemberId FOREIGN KEY(MemberId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'GroupMemberships')
) OPTIONS (NAMEINSOURCE 'CollaborationGroupMember', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CollaborationGroupMemberRequest (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CollaborationGroupId string(18) OPTIONS (NAMEINSOURCE 'CollaborationGroupId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RequesterId string(18) OPTIONS (NAMEINSOURCE 'RequesterId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ResponseMessage string(255) OPTIONS (NAMEINSOURCE 'ResponseMessage', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Pending,Accepted,Declined'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_CollaborationGroup_CollaborationGroupId FOREIGN KEY(CollaborationGroupId) REFERENCES CollaborationGroup (Id) OPTIONS (NAMEINSOURCE 'GroupMemberRequests')
) OPTIONS (NAMEINSOURCE 'CollaborationGroupMemberRequest', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CollaborationInvitation (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SharedEntityId string(18) OPTIONS (NAMEINSOURCE 'SharedEntityId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InviterId string(18) OPTIONS (NAMEINSOURCE 'InviterId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InvitedUserEmail string(240) OPTIONS (NAMEINSOURCE 'InvitedUserEmail', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InvitedUserEmailNormalized string(80) OPTIONS (NAMEINSOURCE 'InvitedUserEmailNormalized', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Sent,Accepted,Canceled'),
    OptionalMessage string(255) OPTIONS (NAMEINSOURCE 'OptionalMessage', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CollaborationInvitation', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Community (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(1000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Community', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Contact (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterRecordId string(18) OPTIONS (NAMEINSOURCE 'MasterRecordId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastName string(80) OPTIONS (NAMEINSOURCE 'LastName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FirstName string(40) OPTIONS (NAMEINSOURCE 'FirstName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Salutation string(40) OPTIONS (NAMEINSOURCE 'Salutation', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Mr.,Ms.,Mrs.,Dr.,Prof.'),
    Name string(121) OPTIONS (NAMEINSOURCE 'Name', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OtherStreet string(255) OPTIONS (NAMEINSOURCE 'OtherStreet', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OtherCity string(40) OPTIONS (NAMEINSOURCE 'OtherCity', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OtherState string(80) OPTIONS (NAMEINSOURCE 'OtherState', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OtherPostalCode string(20) OPTIONS (NAMEINSOURCE 'OtherPostalCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OtherCountry string(80) OPTIONS (NAMEINSOURCE 'OtherCountry', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MailingStreet string(255) OPTIONS (NAMEINSOURCE 'MailingStreet', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MailingCity string(40) OPTIONS (NAMEINSOURCE 'MailingCity', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MailingState string(80) OPTIONS (NAMEINSOURCE 'MailingState', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MailingPostalCode string(20) OPTIONS (NAMEINSOURCE 'MailingPostalCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MailingCountry string(80) OPTIONS (NAMEINSOURCE 'MailingCountry', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Phone string(40) OPTIONS (NAMEINSOURCE 'Phone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Fax string(40) OPTIONS (NAMEINSOURCE 'Fax', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MobilePhone string(40) OPTIONS (NAMEINSOURCE 'MobilePhone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    HomePhone string(40) OPTIONS (NAMEINSOURCE 'HomePhone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OtherPhone string(40) OPTIONS (NAMEINSOURCE 'OtherPhone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AssistantPhone string(40) OPTIONS (NAMEINSOURCE 'AssistantPhone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ReportsToId string(18) OPTIONS (NAMEINSOURCE 'ReportsToId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Email string(80) OPTIONS (NAMEINSOURCE 'Email', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(128) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Department string(80) OPTIONS (NAMEINSOURCE 'Department', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AssistantName string(40) OPTIONS (NAMEINSOURCE 'AssistantName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LeadSource string(40) OPTIONS (NAMEINSOURCE 'LeadSource', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Web,Phone Inquiry,Partner Referral,Purchased List,Other'),
    Birthdate date OPTIONS (NAMEINSOURCE 'Birthdate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LastActivityDate date OPTIONS (NAMEINSOURCE 'LastActivityDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastCURequestDate timestamp OPTIONS (NAMEINSOURCE 'LastCURequestDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastCUUpdateDate timestamp OPTIONS (NAMEINSOURCE 'LastCUUpdateDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EmailBouncedReason string(255) OPTIONS (NAMEINSOURCE 'EmailBouncedReason', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EmailBouncedDate timestamp OPTIONS (NAMEINSOURCE 'EmailBouncedDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Jigsaw string(20) OPTIONS (NAMEINSOURCE 'Jigsaw', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Level__c string(255) OPTIONS (NAMEINSOURCE 'Level__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Secondary,Tertiary,Primary'),
    Languages__c string(100) OPTIONS (NAMEINSOURCE 'Languages__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Contacts')
) OPTIONS (NAMEINSOURCE 'Contact', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'true', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE ContactFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contact_ParentId FOREIGN KEY(ParentId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'ContactFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContactHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,AssistantName,AssistantPhone,Birthdate,contactMerged,created,contactCreatedFromLead,Department,Description,DoNotCall,Email,EmailBouncedDate,EmailBouncedReason,Fax,feedEvent,FirstName,HasOptedOutOfEmail,HasOptedOutOfFax,HomePhone,Jigsaw,Languages__c,LastName,contactUpdatedByLead,LeadSource,Level__c,MailingAddress,MailingCity,MailingCountry,MailingLatitude,MailingLongitude,MailingPostalCode,MailingState,MailingStreet,MobilePhone,Name,OtherAddress,OtherCity,OtherCountry,OtherLatitude,OtherLongitude,OtherPhone,OtherPostalCode,OtherState,OtherStreet,Owner,ownerAccepted,ownerAssignment,Phone,locked,unlocked,ReportsTo,Salutation,Title'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'ContactHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContactShare (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserOrGroupId string(18) OPTIONS (NAMEINSOURCE 'UserOrGroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContactAccessLevel string(40) OPTIONS (NAMEINSOURCE 'ContactAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Read,Edit,All'),
    RowCause string(40) OPTIONS (NAMEINSOURCE 'RowCause', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Owner,Manual,Rule,ImplicitChild,ImplicitParent,Team,Territory,TerritoryManual,TerritoryRule'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Shares')
) OPTIONS (NAMEINSOURCE 'ContactShare', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContentDocument (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PublishStatus string(40) OPTIONS (NAMEINSOURCE 'PublishStatus', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'U,P,R'),
    LatestPublishedVersionId string(18) OPTIONS (NAMEINSOURCE 'LatestPublishedVersionId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ContentDocument', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContentDocumentFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_ContentDocument_ParentId FOREIGN KEY(ParentId) REFERENCES ContentDocument (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'ContentDocumentFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContentDocumentHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContentDocumentId string(18) OPTIONS (NAMEINSOURCE 'ContentDocumentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'ArchivedBy,ArchivedDate,created,DeletedBy,DeletedDate,contentDocPublished,contentDocFeatured,contentDocRepublished,contentDocUnpublished,contentDocSubscribed,contentDocUnsubscribed,feedEvent,IsArchived,Owner,ownerAccepted,ownerAssignment,Parent,PublishStatus,locked,unlocked,Title'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_ContentDocument_ContentDocumentId FOREIGN KEY(ContentDocumentId) REFERENCES ContentDocument (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'ContentDocumentHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContentDocumentLink (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LinkedEntityId string(18) OPTIONS (NAMEINSOURCE 'LinkedEntityId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDocumentId string(18) OPTIONS (NAMEINSOURCE 'ContentDocumentId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ShareType string(40) OPTIONS (NAMEINSOURCE 'ShareType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'V,C,I'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_ContentDocument_ContentDocumentId FOREIGN KEY(ContentDocumentId) REFERENCES ContentDocument (Id) OPTIONS (NAMEINSOURCE 'ContentDocumentLinks')
) OPTIONS (NAMEINSOURCE 'ContentDocumentLink', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContentVersion (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContentDocumentId string(18) OPTIONS (NAMEINSOURCE 'ContentDocumentId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsLatest boolean OPTIONS (NAMEINSOURCE 'IsLatest', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContentUrl string(255) OPTIONS (NAMEINSOURCE 'ContentUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    VersionNumber string(20) OPTIONS (NAMEINSOURCE 'VersionNumber', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(1000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ReasonForChange string(1000) OPTIONS (NAMEINSOURCE 'ReasonForChange', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PathOnClient string(500) OPTIONS (NAMEINSOURCE 'PathOnClient', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RatingCount integer OPTIONS (NAMEINSOURCE 'RatingCount', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContentModifiedDate timestamp OPTIONS (NAMEINSOURCE 'ContentModifiedDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentModifiedById string(18) OPTIONS (NAMEINSOURCE 'ContentModifiedById', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PositiveRatingCount integer OPTIONS (NAMEINSOURCE 'PositiveRatingCount', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NegativeRatingCount integer OPTIONS (NAMEINSOURCE 'NegativeRatingCount', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FeaturedContentBoost integer OPTIONS (NAMEINSOURCE 'FeaturedContentBoost', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FeaturedContentDate date OPTIONS (NAMEINSOURCE 'FeaturedContentDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    TagCsv string(2000) OPTIONS (NAMEINSOURCE 'TagCsv', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FileType string(20) OPTIONS (NAMEINSOURCE 'FileType', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PublishStatus string(40) OPTIONS (NAMEINSOURCE 'PublishStatus', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'U,P,R'),
    VersionData blob OPTIONS (NAMEINSOURCE 'VersionData', UPDATABLE FALSE, NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FirstPublishLocationId string(18) OPTIONS (NAMEINSOURCE 'FirstPublishLocationId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Origin string(40) OPTIONS (NAMEINSOURCE 'Origin', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'C,H'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_ContentDocument_ContentDocumentId FOREIGN KEY(ContentDocumentId) REFERENCES ContentDocument (Id) OPTIONS (NAMEINSOURCE 'ContentVersions')
) OPTIONS (NAMEINSOURCE 'ContentVersion', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE ContentVersionHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContentVersionId string(18) OPTIONS (NAMEINSOURCE 'ContentVersionId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'AvgRating,ContentModifiedBy,ContentModifiedDate,ContentUrl,created,Description,ExternalDataSource,ExternalDocumentInfo1,ExternalDocumentInfo2,FeaturedContentBoost,FeaturedContentDate,feedEvent,FileType,FirstPublishLocation,IsPublic,Language,MaxRating,MinRating,NegativeRatingCount,Owner,ownerAccepted,ownerAssignment,PositiveRatingCount,PublishStatus,RatingCount,ReasonForChange,locked,unlocked,Reference,SuggestedTags,Title,contentVersionCommented,contentVersionCreated,contentVersionDataReplaced,contentVersionDeleted,contentVersionDownloaded,VersionNumber,contentVersionRated,contentVersionUpdated,contentVersionViewed'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_ContentVersion_ContentVersionId FOREIGN KEY(ContentVersionId) REFERENCES ContentVersion (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'ContentVersionHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContentWorkspace (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(500) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TagModel string(40) OPTIONS (NAMEINSOURCE 'TagModel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" ''),
    DefaultRecordTypeId string(18) OPTIONS (NAMEINSOURCE 'DefaultRecordTypeId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsRestrictContentTypes boolean OPTIONS (NAMEINSOURCE 'IsRestrictContentTypes', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsRestrictLinkedContentTypes boolean OPTIONS (NAMEINSOURCE 'IsRestrictLinkedContentTypes', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ContentWorkspace', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContentWorkspaceDoc (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContentWorkspaceId string(18) OPTIONS (NAMEINSOURCE 'ContentWorkspaceId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDocumentId string(18) OPTIONS (NAMEINSOURCE 'ContentDocumentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsOwner boolean OPTIONS (NAMEINSOURCE 'IsOwner', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ContentWorkspaceDoc', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Contract (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerExpirationNotice string(40) OPTIONS (NAMEINSOURCE 'OwnerExpirationNotice', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '15,30,45,60,90,120'),
    StartDate date OPTIONS (NAMEINSOURCE 'StartDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EndDate date OPTIONS (NAMEINSOURCE 'EndDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingStreet string(255) OPTIONS (NAMEINSOURCE 'BillingStreet', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingCity string(40) OPTIONS (NAMEINSOURCE 'BillingCity', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingState string(80) OPTIONS (NAMEINSOURCE 'BillingState', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingPostalCode string(20) OPTIONS (NAMEINSOURCE 'BillingPostalCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BillingCountry string(80) OPTIONS (NAMEINSOURCE 'BillingCountry', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContractTerm integer OPTIONS (NAMEINSOURCE 'ContractTerm', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true', "teiid_sf:Picklist Values" 'In Approval Process,Activated,Draft'),
    CompanySignedId string(18) OPTIONS (NAMEINSOURCE 'CompanySignedId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CompanySignedDate date OPTIONS (NAMEINSOURCE 'CompanySignedDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CustomerSignedId string(18) OPTIONS (NAMEINSOURCE 'CustomerSignedId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CustomerSignedTitle string(40) OPTIONS (NAMEINSOURCE 'CustomerSignedTitle', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CustomerSignedDate date OPTIONS (NAMEINSOURCE 'CustomerSignedDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SpecialTerms string OPTIONS (NAMEINSOURCE 'SpecialTerms', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ActivatedById string(18) OPTIONS (NAMEINSOURCE 'ActivatedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ActivatedDate timestamp OPTIONS (NAMEINSOURCE 'ActivatedDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StatusCode string(40) OPTIONS (NAMEINSOURCE 'StatusCode', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Draft,InApproval,Activated,Terminated,Expired'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContractNumber string(30) OPTIONS (NAMEINSOURCE 'ContractNumber', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LastApprovedDate timestamp OPTIONS (NAMEINSOURCE 'LastApprovedDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastActivityDate date OPTIONS (NAMEINSOURCE 'LastActivityDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Contracts'),
    CONSTRAINT FK_Contact_CustomerSignedId FOREIGN KEY(CustomerSignedId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'ContractsSigned'),
    CONSTRAINT FK_User__CompanySignedId FOREIGN KEY(CompanySignedId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'ContractsSigned')
) OPTIONS (NAMEINSOURCE 'Contract', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE ContractContactRole (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContractId string(18) OPTIONS (NAMEINSOURCE 'ContractId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    "Role" string(40) OPTIONS (NAMEINSOURCE 'Role', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Business User,Decision Maker,Economic Buyer,Economic Decision Maker,Evaluator,Executive Sponsor,Influencer,Technical Buyer,Other'),
    IsPrimary boolean OPTIONS (NAMEINSOURCE 'IsPrimary', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'ContractContactRoles'),
    CONSTRAINT FK_Contract_ContractId FOREIGN KEY(ContractId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'ContractContactRoles')
) OPTIONS (NAMEINSOURCE 'ContractContactRole', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContractFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contract_ParentId FOREIGN KEY(ParentId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'ContractFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContractHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ContractId string(18) OPTIONS (NAMEINSOURCE 'ContractId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,ActivatedBy,ActivatedDate,BillingAddress,BillingCity,BillingCountry,BillingLatitude,BillingLongitude,BillingPostalCode,BillingState,BillingStreet,CompanySigned,CompanySignedDate,contractActivation,contractApproval,contractConversion,contractDraft,contractExpiration,ContractTerm,contractTermination,created,CustomerSigned,CustomerSignedDate,CustomerSignedTitle,Description,EndDate,feedEvent,Name,Owner,ownerAccepted,ownerAssignment,OwnerExpirationNotice,locked,unlocked,ShippingAddress,ShippingCity,ShippingCountry,ShippingLatitude,ShippingLongitude,ShippingPostalCode,ShippingState,ShippingStreet,SpecialTerms,StartDate,Status'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contract_ContractId FOREIGN KEY(ContractId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'ContractHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ContractStatus (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDefault boolean OPTIONS (NAMEINSOURCE 'IsDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    StatusCode string(40) OPTIONS (NAMEINSOURCE 'StatusCode', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Draft,InApproval,Activated,Terminated,Expired'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ContractStatus', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE CronTrigger (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NextFireTime timestamp OPTIONS (NAMEINSOURCE 'NextFireTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PreviousFireTime timestamp OPTIONS (NAMEINSOURCE 'PreviousFireTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    State string(16) OPTIONS (NAMEINSOURCE 'State', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StartTime timestamp OPTIONS (NAMEINSOURCE 'StartTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EndTime timestamp OPTIONS (NAMEINSOURCE 'EndTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CronExpression string(255) OPTIONS (NAMEINSOURCE 'CronExpression', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TimeZoneSidKey string(40) OPTIONS (NAMEINSOURCE 'TimeZoneSidKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Pacific/Kiritimati,Pacific/Enderbury,Pacific/Tongatapu,Pacific/Chatham,Asia/Kamchatka,Pacific/Auckland,Pacific/Fiji,Pacific/Norfolk,Pacific/Guadalcanal,Australia/Lord_Howe,Australia/Brisbane,Australia/Sydney,Australia/Adelaide,Australia/Darwin,Asia/Seoul,Asia/Tokyo,Asia/Hong_Kong,Asia/Kuala_Lumpur,Asia/Manila,Asia/Shanghai,Asia/Singapore,Asia/Taipei,Australia/Perth,Asia/Bangkok,Asia/Ho_Chi_Minh,Asia/Jakarta,Asia/Rangoon,Asia/Dhaka,Asia/Yekaterinburg,Asia/Kathmandu,Asia/Colombo,Asia/Kolkata,Asia/Karachi,Asia/Tashkent,Asia/Kabul,Asia/Tehran,Asia/Dubai,Asia/Tbilisi,Europe/Moscow,Africa/Nairobi,Asia/Baghdad,Asia/Jerusalem,Asia/Kuwait,Asia/Riyadh,Europe/Athens,Europe/Bucharest,Europe/Helsinki,Europe/Istanbul,Europe/Minsk,Africa/Cairo,Africa/Johannesburg,Europe/Amsterdam,Europe/Berlin,Europe/Brussels,Europe/Paris,Europe/Prague,Europe/Rome,Africa/Algiers,Europe/Dublin,Europe/Lisbon,Europe/London,GMT,Atlantic/Cape_Verde,Atlantic/South_Georgia,America/St_Johns,America/Argentina/Buenos_Aires,America/Halifax,America/Sao_Paulo,Atlantic/Bermuda,America/Indiana/Indianapolis,America/New_York,America/Puerto_Rico,America/Santiago,America/Caracas,America/Bogota,America/Chicago,America/Lima,America/Mexico_City,America/Panama,America/Denver,America/El_Salvador,America/Los_Angeles,America/Phoenix,America/Tijuana,America/Anchorage,Pacific/Honolulu,Pacific/Niue,Pacific/Pago_Pago'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TimesTriggered integer OPTIONS (NAMEINSOURCE 'TimesTriggered', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'CronTrigger', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Dashboard (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FolderId string(18) OPTIONS (NAMEINSOURCE 'FolderId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(80) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LeftSize string(40) OPTIONS (NAMEINSOURCE 'LeftSize', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Narrow,Medium,Wide'),
    MiddleSize string(40) OPTIONS (NAMEINSOURCE 'MiddleSize', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Narrow,Medium,Wide'),
    RightSize string(40) OPTIONS (NAMEINSOURCE 'RightSize', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Narrow,Medium,Wide'),
    RunningUserId string(18) OPTIONS (NAMEINSOURCE 'RunningUserId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TitleColor integer OPTIONS (NAMEINSOURCE 'TitleColor', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TitleSize integer OPTIONS (NAMEINSOURCE 'TitleSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TextColor integer OPTIONS (NAMEINSOURCE 'TextColor', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BackgroundStart integer OPTIONS (NAMEINSOURCE 'BackgroundStart', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BackgroundEnd integer OPTIONS (NAMEINSOURCE 'BackgroundEnd', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BackgroundDirection string(40) OPTIONS (NAMEINSOURCE 'BackgroundDirection', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TopToBottom,LeftToRight,Diagonal'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'SpecifiedUser,LoggedInUser,MyTeamUser'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Dashboard', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE DashboardComponent (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DashboardId string(18) OPTIONS (NAMEINSOURCE 'DashboardId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Dashboard_DashboardId FOREIGN KEY(DashboardId) REFERENCES Dashboard (Id) OPTIONS (NAMEINSOURCE 'DashboardComponents')
) OPTIONS (NAMEINSOURCE 'DashboardComponent', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE DashboardComponentFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_DashboardComponent_ParentId FOREIGN KEY(ParentId) REFERENCES DashboardComponent (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'DashboardComponentFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE DashboardFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Dashboard_ParentId FOREIGN KEY(ParentId) REFERENCES Dashboard (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'DashboardFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Document (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FolderId string(18) OPTIONS (NAMEINSOURCE 'FolderId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsPublic boolean OPTIONS (NAMEINSOURCE 'IsPublic', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    BodyLength integer OPTIONS (NAMEINSOURCE 'BodyLength', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body blob OPTIONS (NAMEINSOURCE 'Body', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Url string(255) OPTIONS (NAMEINSOURCE 'Url', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Keywords string(255) OPTIONS (NAMEINSOURCE 'Keywords', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsInternalUseOnly boolean OPTIONS (NAMEINSOURCE 'IsInternalUseOnly', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AuthorId string(18) OPTIONS (NAMEINSOURCE 'AuthorId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsBodySearchable boolean OPTIONS (NAMEINSOURCE 'IsBodySearchable', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Document', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE DocumentAttachmentMap (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DocumentId string(18) OPTIONS (NAMEINSOURCE 'DocumentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DocumentSequence integer OPTIONS (NAMEINSOURCE 'DocumentSequence', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'DocumentAttachmentMap', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE EmailServicesAddress (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LocalPart string(64) OPTIONS (NAMEINSOURCE 'LocalPart', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EmailDomainName string(255) OPTIONS (NAMEINSOURCE 'EmailDomainName', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AuthorizedSenders string OPTIONS (NAMEINSOURCE 'AuthorizedSenders', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RunAsUserId string(18) OPTIONS (NAMEINSOURCE 'RunAsUserId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FunctionId string(18) OPTIONS (NAMEINSOURCE 'FunctionId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_EmailServicesFunction_FunctionId FOREIGN KEY(FunctionId) REFERENCES EmailServicesFunction (Id) OPTIONS (NAMEINSOURCE 'Addresses')
) OPTIONS (NAMEINSOURCE 'EmailServicesAddress', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE EmailServicesFunction (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FunctionName string(64) OPTIONS (NAMEINSOURCE 'FunctionName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AuthorizedSenders string OPTIONS (NAMEINSOURCE 'AuthorizedSenders', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsAuthenticationRequired boolean OPTIONS (NAMEINSOURCE 'IsAuthenticationRequired', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsTlsRequired boolean OPTIONS (NAMEINSOURCE 'IsTlsRequired', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AttachmentOption string(40) OPTIONS (NAMEINSOURCE 'AttachmentOption', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,2,3,4'),
    ApexClassId string(18) OPTIONS (NAMEINSOURCE 'ApexClassId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OverLimitAction string(40) OPTIONS (NAMEINSOURCE 'OverLimitAction', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,2,3'),
    FunctionInactiveAction string(40) OPTIONS (NAMEINSOURCE 'FunctionInactiveAction', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,2,3'),
    AddressInactiveAction string(40) OPTIONS (NAMEINSOURCE 'AddressInactiveAction', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,2,3'),
    AuthenticationFailureAction string(40) OPTIONS (NAMEINSOURCE 'AuthenticationFailureAction', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,2,3'),
    AuthorizationFailureAction string(40) OPTIONS (NAMEINSOURCE 'AuthorizationFailureAction', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,2,3'),
    IsTextTruncated boolean OPTIONS (NAMEINSOURCE 'IsTextTruncated', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsErrorRoutingEnabled boolean OPTIONS (NAMEINSOURCE 'IsErrorRoutingEnabled', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ErrorRoutingAddress string(270) OPTIONS (NAMEINSOURCE 'ErrorRoutingAddress', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsTextAttachmentsAsBinary boolean OPTIONS (NAMEINSOURCE 'IsTextAttachmentsAsBinary', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'EmailServicesFunction', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE EmailStatus (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    TaskId string(18) OPTIONS (NAMEINSOURCE 'TaskId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WhoId string(18) OPTIONS (NAMEINSOURCE 'WhoId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TimesOpened integer OPTIONS (NAMEINSOURCE 'TimesOpened', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FirstOpenDate timestamp OPTIONS (NAMEINSOURCE 'FirstOpenDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastOpenDate timestamp OPTIONS (NAMEINSOURCE 'LastOpenDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EmailTemplateName string(80) OPTIONS (NAMEINSOURCE 'EmailTemplateName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contact_WhoId FOREIGN KEY(WhoId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'EmailStatuses'),
    CONSTRAINT FK_Lead_WhoId FOREIGN KEY(WhoId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'EmailStatuses')
) OPTIONS (NAMEINSOURCE 'EmailStatus', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE EmailTemplate (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FolderId string(18) OPTIONS (NAMEINSOURCE 'FolderId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BrandTemplateId string(18) OPTIONS (NAMEINSOURCE 'BrandTemplateId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TemplateStyle string(40) OPTIONS (NAMEINSOURCE 'TemplateStyle', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'none,freeForm,formalLetter,promotionRight,promotionLeft,newsletter,products'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    TemplateType string(40) OPTIONS (NAMEINSOURCE 'TemplateType', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'text,html,custom,visualforce'),
    Encoding string(40) OPTIONS (NAMEINSOURCE 'Encoding', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'UTF-8,ISO-8859-1,Shift_JIS,ISO-2022-JP,EUC-JP,ks_c_5601-1987,Big5,GB2312'),
    Description string OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Subject string(255) OPTIONS (NAMEINSOURCE 'Subject', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    HtmlValue string(384000) OPTIONS (NAMEINSOURCE 'HtmlValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(384000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TimesUsed integer OPTIONS (NAMEINSOURCE 'TimesUsed', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastUsedDate timestamp OPTIONS (NAMEINSOURCE 'LastUsedDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApiVersion double OPTIONS (NAMEINSOURCE 'ApiVersion', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Markup string(1048576) OPTIONS (NAMEINSOURCE 'Markup', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'EmailTemplate', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE EntitySubscription (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SubscriberId string(18) OPTIONS (NAMEINSOURCE 'SubscriberId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_ParentId FOREIGN KEY(ParentId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Asset_ParentId FOREIGN KEY(ParentId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Campaign_ParentId FOREIGN KEY(ParentId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Case__ParentId FOREIGN KEY(ParentId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_CollaborationGroup_ParentId FOREIGN KEY(ParentId) REFERENCES CollaborationGroup (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Contact_ParentId FOREIGN KEY(ParentId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_ContentDocument_ParentId FOREIGN KEY(ParentId) REFERENCES ContentDocument (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Contract_ParentId FOREIGN KEY(ParentId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Dashboard_ParentId FOREIGN KEY(ParentId) REFERENCES Dashboard (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_DashboardComponent_ParentId FOREIGN KEY(ParentId) REFERENCES DashboardComponent (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Event_ParentId FOREIGN KEY(ParentId) REFERENCES Event (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Lead_ParentId FOREIGN KEY(ParentId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Opportunity_ParentId FOREIGN KEY(ParentId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Product2_ParentId FOREIGN KEY(ParentId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Report_ParentId FOREIGN KEY(ParentId) REFERENCES Report (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Site_ParentId FOREIGN KEY(ParentId) REFERENCES Site (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Solution_ParentId FOREIGN KEY(ParentId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_Task_ParentId FOREIGN KEY(ParentId) REFERENCES Task (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_User__ParentId FOREIGN KEY(ParentId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptionsForEntity'),
    CONSTRAINT FK_User__SubscriberId FOREIGN KEY(SubscriberId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'FeedSubscriptions')
) OPTIONS (NAMEINSOURCE 'EntitySubscription', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Event (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    WhoId string(18) OPTIONS (NAMEINSOURCE 'WhoId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WhatId string(18) OPTIONS (NAMEINSOURCE 'WhatId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Subject string(255) OPTIONS (NAMEINSOURCE 'Subject', NATIVE_TYPE 'combobox', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Location string(255) OPTIONS (NAMEINSOURCE 'Location', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsAllDayEvent boolean OPTIONS (NAMEINSOURCE 'IsAllDayEvent', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ActivityDateTime timestamp OPTIONS (NAMEINSOURCE 'ActivityDateTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ActivityDate date OPTIONS (NAMEINSOURCE 'ActivityDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DurationInMinutes integer OPTIONS (NAMEINSOURCE 'DurationInMinutes', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StartDateTime timestamp OPTIONS (NAMEINSOURCE 'StartDateTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EndDateTime timestamp OPTIONS (NAMEINSOURCE 'EndDateTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsPrivate boolean OPTIONS (NAMEINSOURCE 'IsPrivate', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ShowAs string(40) OPTIONS (NAMEINSOURCE 'ShowAs', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Busy,OutOfOffice,Free'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsChild boolean OPTIONS (NAMEINSOURCE 'IsChild', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsGroupEvent boolean OPTIONS (NAMEINSOURCE 'IsGroupEvent', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    GroupEventType string(40) OPTIONS (NAMEINSOURCE 'GroupEventType', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '0,1,2'),
    IsArchived boolean OPTIONS (NAMEINSOURCE 'IsArchived', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    RecurrenceActivityId string(18) OPTIONS (NAMEINSOURCE 'RecurrenceActivityId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsRecurrence boolean OPTIONS (NAMEINSOURCE 'IsRecurrence', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    RecurrenceStartDateTime timestamp OPTIONS (NAMEINSOURCE 'RecurrenceStartDateTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceEndDateOnly date OPTIONS (NAMEINSOURCE 'RecurrenceEndDateOnly', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceTimeZoneSidKey string(40) OPTIONS (NAMEINSOURCE 'RecurrenceTimeZoneSidKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Pacific/Kiritimati,Pacific/Enderbury,Pacific/Tongatapu,Pacific/Chatham,Asia/Kamchatka,Pacific/Auckland,Pacific/Fiji,Pacific/Norfolk,Pacific/Guadalcanal,Australia/Lord_Howe,Australia/Brisbane,Australia/Sydney,Australia/Adelaide,Australia/Darwin,Asia/Seoul,Asia/Tokyo,Asia/Hong_Kong,Asia/Kuala_Lumpur,Asia/Manila,Asia/Shanghai,Asia/Singapore,Asia/Taipei,Australia/Perth,Asia/Bangkok,Asia/Ho_Chi_Minh,Asia/Jakarta,Asia/Rangoon,Asia/Dhaka,Asia/Yekaterinburg,Asia/Kathmandu,Asia/Colombo,Asia/Kolkata,Asia/Karachi,Asia/Tashkent,Asia/Kabul,Asia/Tehran,Asia/Dubai,Asia/Tbilisi,Europe/Moscow,Africa/Nairobi,Asia/Baghdad,Asia/Jerusalem,Asia/Kuwait,Asia/Riyadh,Europe/Athens,Europe/Bucharest,Europe/Helsinki,Europe/Istanbul,Europe/Minsk,Africa/Cairo,Africa/Johannesburg,Europe/Amsterdam,Europe/Berlin,Europe/Brussels,Europe/Paris,Europe/Prague,Europe/Rome,Africa/Algiers,Europe/Dublin,Europe/Lisbon,Europe/London,GMT,Atlantic/Cape_Verde,Atlantic/South_Georgia,America/St_Johns,America/Argentina/Buenos_Aires,America/Halifax,America/Sao_Paulo,Atlantic/Bermuda,America/Indiana/Indianapolis,America/New_York,America/Puerto_Rico,America/Santiago,America/Caracas,America/Bogota,America/Chicago,America/Lima,America/Mexico_City,America/Panama,America/Denver,America/El_Salvador,America/Los_Angeles,America/Phoenix,America/Tijuana,America/Anchorage,Pacific/Honolulu,Pacific/Niue,Pacific/Pago_Pago'),
    RecurrenceType string(40) OPTIONS (NAMEINSOURCE 'RecurrenceType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'RecursDaily,RecursEveryWeekday,RecursMonthly,RecursMonthlyNth,RecursWeekly,RecursYearly,RecursYearlyNth'),
    RecurrenceInterval integer OPTIONS (NAMEINSOURCE 'RecurrenceInterval', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceDayOfWeekMask integer OPTIONS (NAMEINSOURCE 'RecurrenceDayOfWeekMask', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceDayOfMonth integer OPTIONS (NAMEINSOURCE 'RecurrenceDayOfMonth', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceInstance string(40) OPTIONS (NAMEINSOURCE 'RecurrenceInstance', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'First,Second,Third,Fourth,Last'),
    RecurrenceMonthOfYear string(40) OPTIONS (NAMEINSOURCE 'RecurrenceMonthOfYear', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'January,February,March,April,May,June,July,August,September,October,November,December'),
    ReminderDateTime timestamp OPTIONS (NAMEINSOURCE 'ReminderDateTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsReminderSet boolean OPTIONS (NAMEINSOURCE 'IsReminderSet', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_WhatId FOREIGN KEY(WhatId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Asset_WhatId FOREIGN KEY(WhatId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Campaign_WhatId FOREIGN KEY(WhatId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Case__WhatId FOREIGN KEY(WhatId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Contact_WhoId FOREIGN KEY(WhoId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Contract_WhatId FOREIGN KEY(WhatId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Event_RecurrenceActivityId FOREIGN KEY(RecurrenceActivityId) REFERENCES Event (Id) OPTIONS (NAMEINSOURCE 'RecurringEvents'),
    CONSTRAINT FK_Lead_WhoId FOREIGN KEY(WhoId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Opportunity_WhatId FOREIGN KEY(WhatId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Product2_WhatId FOREIGN KEY(WhatId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'Events'),
    CONSTRAINT FK_Solution_WhatId FOREIGN KEY(WhatId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'Events')
) OPTIONS (NAMEINSOURCE 'Event', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE EventAttendee (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    EventId string(18) OPTIONS (NAMEINSOURCE 'EventId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AttendeeId string(18) OPTIONS (NAMEINSOURCE 'AttendeeId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'New,Declined,Accepted,Uninvited,Maybe'),
    RespondedDate timestamp OPTIONS (NAMEINSOURCE 'RespondedDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Response string(255) OPTIONS (NAMEINSOURCE 'Response', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Event_EventId FOREIGN KEY(EventId) REFERENCES Event (Id) OPTIONS (NAMEINSOURCE 'EventAttendees')
) OPTIONS (NAMEINSOURCE 'EventAttendee', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE EventFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Event_ParentId FOREIGN KEY(ParentId) REFERENCES Event (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'EventFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE FeedComment (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FeedItemId string(18) OPTIONS (NAMEINSOURCE 'FeedItemId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CommentBody string(5000) OPTIONS (NAMEINSOURCE 'CommentBody', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_AccountFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES AccountFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_AssetFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES AssetFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_CampaignFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CampaignFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_CaseFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CaseFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_CollaborationGroupFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CollaborationGroupFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_ContactFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContactFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_ContentDocumentFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContentDocumentFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_ContractFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContractFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_DashboardComponentFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES DashboardComponentFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_DashboardFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES DashboardFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_EventFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES EventFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_FeedItem_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES FeedItem (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_LeadFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES LeadFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_NewsFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES NewsFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_OpportunityFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES OpportunityFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_Product2Feed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES Product2Feed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_ReportFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ReportFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_SiteFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES SiteFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_SolutionFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES SolutionFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_TaskFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES TaskFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_UserFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES UserFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments'),
    CONSTRAINT FK_UserProfileFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES UserProfileFeed (Id) OPTIONS (NAMEINSOURCE 'FeedComments')
) OPTIONS (NAMEINSOURCE 'FeedComment', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE FeedItem (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'FeedItem', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE FeedLike (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FeedItemId string(18) OPTIONS (NAMEINSOURCE 'FeedItemId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_AccountFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES AccountFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_AssetFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES AssetFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_CampaignFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CampaignFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_CaseFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CaseFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_CollaborationGroupFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CollaborationGroupFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_ContactFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContactFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_ContentDocumentFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContentDocumentFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_ContractFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContractFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_DashboardComponentFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES DashboardComponentFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_DashboardFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES DashboardFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_EventFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES EventFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_FeedItem_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES FeedItem (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_LeadFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES LeadFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_NewsFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES NewsFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_OpportunityFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES OpportunityFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_Product2Feed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES Product2Feed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_ReportFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ReportFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_SiteFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES SiteFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_SolutionFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES SolutionFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_TaskFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES TaskFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_UserFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES UserFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes'),
    CONSTRAINT FK_UserProfileFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES UserProfileFeed (Id) OPTIONS (NAMEINSOURCE 'FeedLikes')
) OPTIONS (NAMEINSOURCE 'FeedLike', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE FeedTrackedChange (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FeedItemId string(18) OPTIONS (NAMEINSOURCE 'FeedItemId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FieldName string(120) OPTIONS (NAMEINSOURCE 'FieldName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_AccountFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES AccountFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_AssetFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES AssetFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_CampaignFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CampaignFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_CaseFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CaseFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_CollaborationGroupFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES CollaborationGroupFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_ContactFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContactFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_ContentDocumentFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContentDocumentFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_ContractFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ContractFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_DashboardComponentFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES DashboardComponentFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_DashboardFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES DashboardFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_EventFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES EventFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_FeedItem_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES FeedItem (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_LeadFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES LeadFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_NewsFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES NewsFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_OpportunityFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES OpportunityFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_Product2Feed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES Product2Feed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_ReportFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES ReportFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_SiteFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES SiteFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_SolutionFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES SolutionFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_TaskFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES TaskFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_UserFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES UserFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges'),
    CONSTRAINT FK_UserProfileFeed_FeedItemId FOREIGN KEY(FeedItemId) REFERENCES UserProfileFeed (Id) OPTIONS (NAMEINSOURCE 'FeedTrackedChanges')
) OPTIONS (NAMEINSOURCE 'FeedTrackedChange', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE FiscalYearSettings (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    PeriodId string(18) OPTIONS (NAMEINSOURCE 'PeriodId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StartDate date OPTIONS (NAMEINSOURCE 'StartDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EndDate date OPTIONS (NAMEINSOURCE 'EndDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsStandardYear boolean OPTIONS (NAMEINSOURCE 'IsStandardYear', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    YearType string(40) OPTIONS (NAMEINSOURCE 'YearType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Standard,Custom,Placeholder'),
    QuarterLabelScheme string(40) OPTIONS (NAMEINSOURCE 'QuarterLabelScheme', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'NumberByYear,Custom'),
    PeriodLabelScheme string(40) OPTIONS (NAMEINSOURCE 'PeriodLabelScheme', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'NumberByYear,NumberByQuarter,StandardMonths,Custom'),
    WeekLabelScheme string(40) OPTIONS (NAMEINSOURCE 'WeekLabelScheme', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'NumberByYear,NumberByQuarter,NumberByPeriod'),
    QuarterPrefix string(40) OPTIONS (NAMEINSOURCE 'QuarterPrefix', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Quarter,FQ,Q,Trimester'),
    PeriodPrefix string(40) OPTIONS (NAMEINSOURCE 'PeriodPrefix', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Period,FP,P,Month'),
    WeekStartDay integer OPTIONS (NAMEINSOURCE 'WeekStartDay', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'FiscalYearSettings', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Folder (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(40) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccessType string(40) OPTIONS (NAMEINSOURCE 'AccessType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Shared,Public,Hidden,PublicInternal'),
    IsReadonly boolean OPTIONS (NAMEINSOURCE 'IsReadonly', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Document,Email,Report,Dashboard'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Folder', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ForecastShare (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    UserRoleId string(18) OPTIONS (NAMEINSOURCE 'UserRoleId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserOrGroupId string(18) OPTIONS (NAMEINSOURCE 'UserOrGroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccessLevel string(40) OPTIONS (NAMEINSOURCE 'AccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Read,Edit,All'),
    CanSubmit boolean OPTIONS (NAMEINSOURCE 'CanSubmit', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    RowCause string(40) OPTIONS (NAMEINSOURCE 'RowCause', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Owner,Manual,Rule,ImplicitChild,ImplicitParent,Team,Territory,TerritoryManual,TerritoryRule'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ForecastShare', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Group_ (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(40) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedId string(18) OPTIONS (NAMEINSOURCE 'RelatedId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'AllCustomerPortal,CollaborationGroup,Manager,ManagerAndSubordinatesInternal,Organization,PRMOrganization,Queue,Regular,Role,RoleAndSubordinates,RoleAndSubordinatesInternal,SharingRuleGroup,Territory,TerritoryAndSubordinates'),
    Email string(255) OPTIONS (NAMEINSOURCE 'Email', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DoesSendEmailToMembers boolean OPTIONS (NAMEINSOURCE 'DoesSendEmailToMembers', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    DoesIncludeBosses boolean OPTIONS (NAMEINSOURCE 'DoesIncludeBosses', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Group', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE GroupMember (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    GroupId string(18) OPTIONS (NAMEINSOURCE 'GroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserOrGroupId string(18) OPTIONS (NAMEINSOURCE 'UserOrGroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Group__GroupId FOREIGN KEY(GroupId) REFERENCES Group_ (Id) OPTIONS (NAMEINSOURCE 'GroupMembers')
) OPTIONS (NAMEINSOURCE 'GroupMember', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Holiday (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(100) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsAllDay boolean OPTIONS (NAMEINSOURCE 'IsAllDay', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ActivityDate date OPTIONS (NAMEINSOURCE 'ActivityDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StartTimeInMinutes integer OPTIONS (NAMEINSOURCE 'StartTimeInMinutes', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EndTimeInMinutes integer OPTIONS (NAMEINSOURCE 'EndTimeInMinutes', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsRecurrence boolean OPTIONS (NAMEINSOURCE 'IsRecurrence', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    RecurrenceStartDate date OPTIONS (NAMEINSOURCE 'RecurrenceStartDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceEndDateOnly date OPTIONS (NAMEINSOURCE 'RecurrenceEndDateOnly', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceType string(40) OPTIONS (NAMEINSOURCE 'RecurrenceType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'RecursDaily,RecursEveryWeekday,RecursMonthly,RecursMonthlyNth,RecursWeekly,RecursYearly,RecursYearlyNth'),
    RecurrenceInterval integer OPTIONS (NAMEINSOURCE 'RecurrenceInterval', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceDayOfWeekMask integer OPTIONS (NAMEINSOURCE 'RecurrenceDayOfWeekMask', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceDayOfMonth integer OPTIONS (NAMEINSOURCE 'RecurrenceDayOfMonth', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceInstance string(40) OPTIONS (NAMEINSOURCE 'RecurrenceInstance', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'First,Second,Third,Fourth,Last'),
    RecurrenceMonthOfYear string(40) OPTIONS (NAMEINSOURCE 'RecurrenceMonthOfYear', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'January,February,March,April,May,June,July,August,September,October,November,December'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Holiday', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Idea (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecordTypeId string(18) OPTIONS (NAMEINSOURCE 'RecordTypeId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsLocked boolean OPTIONS (NAMEINSOURCE 'IsLocked', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommunityId string(18) OPTIONS (NAMEINSOURCE 'CommunityId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(32000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumComments integer OPTIONS (NAMEINSOURCE 'NumComments', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'true', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    VoteScore double OPTIONS (NAMEINSOURCE 'VoteScore', UPDATABLE FALSE, NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    VoteTotal double OPTIONS (NAMEINSOURCE 'VoteTotal', UPDATABLE FALSE, NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Categories string(4099) OPTIONS (NAMEINSOURCE 'Categories', NATIVE_TYPE 'multipicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" ''),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" ''),
    LastCommentDate timestamp OPTIONS (NAMEINSOURCE 'LastCommentDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'true', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastCommentId string(18) OPTIONS (NAMEINSOURCE 'LastCommentId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ParentIdeaId string(18) OPTIONS (NAMEINSOURCE 'ParentIdeaId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsHtml boolean OPTIONS (NAMEINSOURCE 'IsHtml', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Idea', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE IdeaComment (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IdeaId string(18) OPTIONS (NAMEINSOURCE 'IdeaId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CommentBody string OPTIONS (NAMEINSOURCE 'CommentBody', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsHtml boolean OPTIONS (NAMEINSOURCE 'IsHtml', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Idea_IdeaId FOREIGN KEY(IdeaId) REFERENCES Idea (Id) OPTIONS (NAMEINSOURCE 'Comments')
) OPTIONS (NAMEINSOURCE 'IdeaComment', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE Lead (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterRecordId string(18) OPTIONS (NAMEINSOURCE 'MasterRecordId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastName string(80) OPTIONS (NAMEINSOURCE 'LastName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FirstName string(40) OPTIONS (NAMEINSOURCE 'FirstName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Salutation string(40) OPTIONS (NAMEINSOURCE 'Salutation', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Mr.,Ms.,Mrs.,Dr.,Prof.'),
    Name string(121) OPTIONS (NAMEINSOURCE 'Name', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(128) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Company string(255) OPTIONS (NAMEINSOURCE 'Company', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Street string(255) OPTIONS (NAMEINSOURCE 'Street', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    City string(40) OPTIONS (NAMEINSOURCE 'City', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    State string(80) OPTIONS (NAMEINSOURCE 'State', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PostalCode string(20) OPTIONS (NAMEINSOURCE 'PostalCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Country string(80) OPTIONS (NAMEINSOURCE 'Country', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Phone string(40) OPTIONS (NAMEINSOURCE 'Phone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MobilePhone string(40) OPTIONS (NAMEINSOURCE 'MobilePhone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Fax string(40) OPTIONS (NAMEINSOURCE 'Fax', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Email string(80) OPTIONS (NAMEINSOURCE 'Email', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Website string(255) OPTIONS (NAMEINSOURCE 'Website', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LeadSource string(40) OPTIONS (NAMEINSOURCE 'LeadSource', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Web,Phone Inquiry,Partner Referral,Purchased List,Other'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true', "teiid_sf:Picklist Values" 'Open - Not Contacted,Working - Contacted,Closed - Converted,Closed - Not Converted'),
    Industry string(40) OPTIONS (NAMEINSOURCE 'Industry', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Agriculture,Apparel,Banking,Biotechnology,Chemicals,Communications,Construction,Consulting,Education,Electronics,Energy,Engineering,Entertainment,Environmental,Finance,Food & Beverage,Government,Healthcare,Hospitality,Insurance,Machinery,Manufacturing,Media,Not For Profit,Recreation,Retail,Shipping,Technology,Telecommunications,Transportation,Utilities,Other'),
    Rating string(40) OPTIONS (NAMEINSOURCE 'Rating', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Hot,Warm,Cold'),
    AnnualRevenue double OPTIONS (NAMEINSOURCE 'AnnualRevenue', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NumberOfEmployees integer OPTIONS (NAMEINSOURCE 'NumberOfEmployees', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsConverted boolean OPTIONS (NAMEINSOURCE 'IsConverted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ConvertedDate date OPTIONS (NAMEINSOURCE 'ConvertedDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ConvertedAccountId string(18) OPTIONS (NAMEINSOURCE 'ConvertedAccountId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ConvertedContactId string(18) OPTIONS (NAMEINSOURCE 'ConvertedContactId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ConvertedOpportunityId string(18) OPTIONS (NAMEINSOURCE 'ConvertedOpportunityId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsUnreadByOwner boolean OPTIONS (NAMEINSOURCE 'IsUnreadByOwner', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LastActivityDate date OPTIONS (NAMEINSOURCE 'LastActivityDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Jigsaw string(20) OPTIONS (NAMEINSOURCE 'Jigsaw', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EmailBouncedReason string(255) OPTIONS (NAMEINSOURCE 'EmailBouncedReason', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EmailBouncedDate timestamp OPTIONS (NAMEINSOURCE 'EmailBouncedDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SICCode__c string(15) OPTIONS (NAMEINSOURCE 'SICCode__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    ProductInterest__c string(255) OPTIONS (NAMEINSOURCE 'ProductInterest__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'GC1000 series,GC5000 series,GC3000 series'),
    Primary__c string(255) OPTIONS (NAMEINSOURCE 'Primary__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'No,Yes'),
    CurrentGenerators__c string(100) OPTIONS (NAMEINSOURCE 'CurrentGenerators__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    NumberofLocations__c double OPTIONS (NAMEINSOURCE 'NumberofLocations__c', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Lead', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'true', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE LeadFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Lead_ParentId FOREIGN KEY(ParentId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'LeadFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE LeadHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LeadId string(18) OPTIONS (NAMEINSOURCE 'LeadId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Address,AnnualRevenue,City,Company,Country,created,CurrentGenerators__c,Description,DoNotCall,Email,EmailBouncedDate,EmailBouncedReason,Fax,feedEvent,FirstName,HasOptedOutOfEmail,HasOptedOutOfFax,Industry,IsUnreadByOwner,Jigsaw,LastName,Latitude,leadConverted,leadMerged,LeadSource,Longitude,MobilePhone,Name,NumberOfEmployees,NumberofLocations__c,Owner,ownerAccepted,ownerAssignment,Phone,PostalCode,Primary__c,ProductInterest__c,Rating,locked,unlocked,Salutation,SICCode__c,State,Status,Street,Title,Website'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Lead_LeadId FOREIGN KEY(LeadId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'LeadHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE LeadShare (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LeadId string(18) OPTIONS (NAMEINSOURCE 'LeadId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserOrGroupId string(18) OPTIONS (NAMEINSOURCE 'UserOrGroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LeadAccessLevel string(40) OPTIONS (NAMEINSOURCE 'LeadAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Read,Edit,All'),
    RowCause string(40) OPTIONS (NAMEINSOURCE 'RowCause', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Owner,Manual,Rule,ImplicitChild,ImplicitParent,Team,Territory,TerritoryManual,TerritoryRule'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Lead_LeadId FOREIGN KEY(LeadId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'Shares')
) OPTIONS (NAMEINSOURCE 'LeadShare', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE LeadStatus (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDefault boolean OPTIONS (NAMEINSOURCE 'IsDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsConverted boolean OPTIONS (NAMEINSOURCE 'IsConverted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'LeadStatus', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE LoginHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    UserId string(18) OPTIONS (NAMEINSOURCE 'UserId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LoginTime timestamp OPTIONS (NAMEINSOURCE 'LoginTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LoginType string(1) OPTIONS (NAMEINSOURCE 'LoginType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" ''),
    SourceIp string(39) OPTIONS (NAMEINSOURCE 'SourceIp', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LoginUrl string(120) OPTIONS (NAMEINSOURCE 'LoginUrl', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Browser string(64) OPTIONS (NAMEINSOURCE 'Browser', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Platform string(64) OPTIONS (NAMEINSOURCE 'Platform', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(128) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Application string(64) OPTIONS (NAMEINSOURCE 'Application', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ClientVersion string(64) OPTIONS (NAMEINSOURCE 'ClientVersion', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApiType string(64) OPTIONS (NAMEINSOURCE 'ApiType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApiVersion string(32) OPTIONS (NAMEINSOURCE 'ApiVersion', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'LoginHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE LoginIp (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    UsersId string(18) OPTIONS (NAMEINSOURCE 'UsersId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SourceIp string(39) OPTIONS (NAMEINSOURCE 'SourceIp', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsAuthenticated boolean OPTIONS (NAMEINSOURCE 'IsAuthenticated', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ChallengeSentDate timestamp OPTIONS (NAMEINSOURCE 'ChallengeSentDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'LoginIp', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE MailmergeTemplate (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Filename string(255) OPTIONS (NAMEINSOURCE 'Filename', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BodyLength integer OPTIONS (NAMEINSOURCE 'BodyLength', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body blob OPTIONS (NAMEINSOURCE 'Body', UPDATABLE FALSE, NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastUsedDate timestamp OPTIONS (NAMEINSOURCE 'LastUsedDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'MailmergeTemplate', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Name (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastName string(80) OPTIONS (NAMEINSOURCE 'LastName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FirstName string(40) OPTIONS (NAMEINSOURCE 'FirstName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,AccountContactRole,Asset,Campaign,Case,CollaborationGroup,Contact,ContentDocument,ContentVersion,Contract,Dashboard,DashboardComponent,Document,Event,Idea,IdeaComment,Lead,Opportunity,Pricebook2,Product2,Queue,Report,SelfServiceUser,Solution,Task,User'),
    Alias string(8) OPTIONS (NAMEINSOURCE 'Alias', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserRoleId string(18) OPTIONS (NAMEINSOURCE 'UserRoleId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ProfileId string(18) OPTIONS (NAMEINSOURCE 'ProfileId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(80) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Email string(80) OPTIONS (NAMEINSOURCE 'Email', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Phone string(40) OPTIONS (NAMEINSOURCE 'Phone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Username string(80) OPTIONS (NAMEINSOURCE 'Username', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Name', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE NewsFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'NewsFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Note (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(80) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsPrivate boolean OPTIONS (NAMEINSOURCE 'IsPrivate', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Body string(32000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_ParentId FOREIGN KEY(ParentId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Notes'),
    CONSTRAINT FK_Asset_ParentId FOREIGN KEY(ParentId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'Notes'),
    CONSTRAINT FK_Contact_ParentId FOREIGN KEY(ParentId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Notes'),
    CONSTRAINT FK_Contract_ParentId FOREIGN KEY(ParentId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'Notes'),
    CONSTRAINT FK_Lead_ParentId FOREIGN KEY(ParentId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'Notes'),
    CONSTRAINT FK_Opportunity_ParentId FOREIGN KEY(ParentId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'Notes'),
    CONSTRAINT FK_Product2_ParentId FOREIGN KEY(ParentId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'Notes')
) OPTIONS (NAMEINSOURCE 'Note', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE NoteAndAttachment (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsNote boolean OPTIONS (NAMEINSOURCE 'IsNote', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(80) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsPrivate boolean OPTIONS (NAMEINSOURCE 'IsPrivate', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_ParentId FOREIGN KEY(ParentId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'NotesAndAttachments'),
    CONSTRAINT FK_Asset_ParentId FOREIGN KEY(ParentId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'NotesAndAttachments'),
    CONSTRAINT FK_Contact_ParentId FOREIGN KEY(ParentId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'NotesAndAttachments'),
    CONSTRAINT FK_Contract_ParentId FOREIGN KEY(ParentId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'NotesAndAttachments'),
    CONSTRAINT FK_Lead_ParentId FOREIGN KEY(ParentId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'NotesAndAttachments'),
    CONSTRAINT FK_Opportunity_ParentId FOREIGN KEY(ParentId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'NotesAndAttachments'),
    CONSTRAINT FK_Product2_ParentId FOREIGN KEY(ParentId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'NotesAndAttachments')
) OPTIONS (NAMEINSOURCE 'NoteAndAttachment', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpenActivity (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WhoId string(18) OPTIONS (NAMEINSOURCE 'WhoId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WhatId string(18) OPTIONS (NAMEINSOURCE 'WhatId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Subject string(80) OPTIONS (NAMEINSOURCE 'Subject', NATIVE_TYPE 'combobox', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsTask boolean OPTIONS (NAMEINSOURCE 'IsTask', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ActivityDate date OPTIONS (NAMEINSOURCE 'ActivityDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Not Started,In Progress,Completed,Waiting on someone else,Deferred'),
    Priority string(40) OPTIONS (NAMEINSOURCE 'Priority', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'High,Normal,Low'),
    ActivityType string(40) OPTIONS (NAMEINSOURCE 'ActivityType', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Call,Call,Email,Email,Meeting,Meeting,Other,Other'),
    IsClosed boolean OPTIONS (NAMEINSOURCE 'IsClosed', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsAllDayEvent boolean OPTIONS (NAMEINSOURCE 'IsAllDayEvent', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsVisibleInSelfService boolean OPTIONS (NAMEINSOURCE 'IsVisibleInSelfService', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    DurationInMinutes integer OPTIONS (NAMEINSOURCE 'DurationInMinutes', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Location string(80) OPTIONS (NAMEINSOURCE 'Location', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CallDurationInSeconds integer OPTIONS (NAMEINSOURCE 'CallDurationInSeconds', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CallType string(40) OPTIONS (NAMEINSOURCE 'CallType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Internal,Inbound,Outbound'),
    CallDisposition string(255) OPTIONS (NAMEINSOURCE 'CallDisposition', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CallObject string(255) OPTIONS (NAMEINSOURCE 'CallObject', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ReminderDateTime timestamp OPTIONS (NAMEINSOURCE 'ReminderDateTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsReminderSet boolean OPTIONS (NAMEINSOURCE 'IsReminderSet', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Asset_WhatId FOREIGN KEY(WhatId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Campaign_WhatId FOREIGN KEY(WhatId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Case__WhatId FOREIGN KEY(WhatId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Contact_WhoId FOREIGN KEY(WhoId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Contract_WhatId FOREIGN KEY(WhatId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Lead_WhoId FOREIGN KEY(WhoId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Opportunity_WhatId FOREIGN KEY(WhatId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Product2_WhatId FOREIGN KEY(WhatId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'OpenActivities'),
    CONSTRAINT FK_Solution_WhatId FOREIGN KEY(WhatId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'OpenActivities')
) OPTIONS (NAMEINSOURCE 'OpenActivity', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Opportunity (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsPrivate boolean OPTIONS (NAMEINSOURCE 'IsPrivate', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(120) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StageName string(40) OPTIONS (NAMEINSOURCE 'StageName', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Prospecting,Qualification,Needs Analysis,Value Proposition,Id. Decision Makers,Perception Analysis,Proposal/Price Quote,Negotiation/Review,Closed Won,Closed Lost'),
    Amount double OPTIONS (NAMEINSOURCE 'Amount', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Probability double OPTIONS (NAMEINSOURCE 'Probability', NATIVE_TYPE 'percent', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ExpectedRevenue double OPTIONS (NAMEINSOURCE 'ExpectedRevenue', UPDATABLE FALSE, CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TotalOpportunityQuantity double OPTIONS (NAMEINSOURCE 'TotalOpportunityQuantity', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CloseDate date OPTIONS (NAMEINSOURCE 'CloseDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Existing Customer - Upgrade,Existing Customer - Replacement,Existing Customer - Downgrade,New Customer'),
    NextStep string(255) OPTIONS (NAMEINSOURCE 'NextStep', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LeadSource string(40) OPTIONS (NAMEINSOURCE 'LeadSource', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Web,Phone Inquiry,Partner Referral,Purchased List,Other'),
    IsClosed boolean OPTIONS (NAMEINSOURCE 'IsClosed', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsWon boolean OPTIONS (NAMEINSOURCE 'IsWon', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ForecastCategory string(40) OPTIONS (NAMEINSOURCE 'ForecastCategory', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Omitted,Pipeline,BestCase,Forecast,Closed'),
    ForecastCategoryName string(40) OPTIONS (NAMEINSOURCE 'ForecastCategoryName', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true', "teiid_sf:Picklist Values" 'Omitted,Pipeline,Best Case,Commit,Closed'),
    CampaignId string(18) OPTIONS (NAMEINSOURCE 'CampaignId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    HasOpportunityLineItem boolean OPTIONS (NAMEINSOURCE 'HasOpportunityLineItem', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Pricebook2Id string(18) OPTIONS (NAMEINSOURCE 'Pricebook2Id', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LastActivityDate date OPTIONS (NAMEINSOURCE 'LastActivityDate', UPDATABLE FALSE, NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FiscalQuarter integer OPTIONS (NAMEINSOURCE 'FiscalQuarter', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FiscalYear integer OPTIONS (NAMEINSOURCE 'FiscalYear', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Fiscal string(6) OPTIONS (NAMEINSOURCE 'Fiscal', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeliveryInstallationStatus__c string(255) OPTIONS (NAMEINSOURCE 'DeliveryInstallationStatus__c', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'In progress,Yet to begin,Completed'),
    TrackingNumber__c string(12) OPTIONS (NAMEINSOURCE 'TrackingNumber__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    OrderNumber__c string(8) OPTIONS (NAMEINSOURCE 'OrderNumber__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    CurrentGenerators__c string(100) OPTIONS (NAMEINSOURCE 'CurrentGenerators__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    MainCompetitors__c string(100) OPTIONS (NAMEINSOURCE 'MainCompetitors__c', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountId FOREIGN KEY(AccountId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Opportunities'),
    CONSTRAINT FK_Campaign_CampaignId FOREIGN KEY(CampaignId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'Opportunities'),
    CONSTRAINT FK_Pricebook2_Pricebook2Id FOREIGN KEY(Pricebook2Id) REFERENCES Pricebook2 (Id) OPTIONS (NAMEINSOURCE 'Opportunities')
) OPTIONS (NAMEINSOURCE 'Opportunity', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE OpportunityCompetitor (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CompetitorName string(40) OPTIONS (NAMEINSOURCE 'CompetitorName', NATIVE_TYPE 'combobox', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Strengths string(1000) OPTIONS (NAMEINSOURCE 'Strengths', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Weaknesses string(1000) OPTIONS (NAMEINSOURCE 'Weaknesses', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'OpportunityCompetitors')
) OPTIONS (NAMEINSOURCE 'OpportunityCompetitor', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpportunityContactRole (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    "Role" string(40) OPTIONS (NAMEINSOURCE 'Role', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Business User,Decision Maker,Economic Buyer,Economic Decision Maker,Evaluator,Executive Sponsor,Influencer,Technical Buyer,Other'),
    IsPrimary boolean OPTIONS (NAMEINSOURCE 'IsPrimary', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Contact_ContactId FOREIGN KEY(ContactId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'OpportunityContactRoles'),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'OpportunityContactRoles')
) OPTIONS (NAMEINSOURCE 'OpportunityContactRole', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpportunityFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Opportunity_ParentId FOREIGN KEY(ParentId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'OpportunityFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpportunityFieldHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,Amount,Campaign,CloseDate,Contract,created,opportunityCreatedFromLead,CurrentGenerators__c,DeliveryInstallationStatus__c,Description,feedEvent,ForecastCategoryName,IsPrivate,LeadSource,MainCompetitors__c,Name,NextStep,OrderNumber__c,Owner,ownerAccepted,ownerAssignment,Probability,locked,unlocked,StageName,TotalOpportunityQuantity,TrackingNumber__c,Type'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'OpportunityFieldHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpportunityHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StageName string(40) OPTIONS (NAMEINSOURCE 'StageName', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Prospecting,Qualification,Needs Analysis,Value Proposition,Id. Decision Makers,Perception Analysis,Proposal/Price Quote,Negotiation/Review,Closed Won,Closed Lost'),
    Amount double OPTIONS (NAMEINSOURCE 'Amount', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ExpectedRevenue double OPTIONS (NAMEINSOURCE 'ExpectedRevenue', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CloseDate date OPTIONS (NAMEINSOURCE 'CloseDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Probability double OPTIONS (NAMEINSOURCE 'Probability', NATIVE_TYPE 'percent', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ForecastCategory string(40) OPTIONS (NAMEINSOURCE 'ForecastCategory', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Omitted,Pipeline,BestCase,Forecast,Closed'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'OpportunityHistories')
) OPTIONS (NAMEINSOURCE 'OpportunityHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpportunityLineItem (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PricebookEntryId string(18) OPTIONS (NAMEINSOURCE 'PricebookEntryId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Quantity double OPTIONS (NAMEINSOURCE 'Quantity', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TotalPrice double OPTIONS (NAMEINSOURCE 'TotalPrice', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    UnitPrice double OPTIONS (NAMEINSOURCE 'UnitPrice', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ListPrice double OPTIONS (NAMEINSOURCE 'ListPrice', UPDATABLE FALSE, CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ServiceDate date OPTIONS (NAMEINSOURCE 'ServiceDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'OpportunityLineItems'),
    CONSTRAINT FK_PricebookEntry_PricebookEntryId FOREIGN KEY(PricebookEntryId) REFERENCES PricebookEntry (Id) OPTIONS (NAMEINSOURCE 'OpportunityLineItems')
) OPTIONS (NAMEINSOURCE 'OpportunityLineItem', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpportunityPartner (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountToId string(18) OPTIONS (NAMEINSOURCE 'AccountToId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    "Role" string(40) OPTIONS (NAMEINSOURCE 'Role', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'System Integrator,Agency,Advertiser,VAR/Reseller,Distributor,Developer,Broker,Lender,Supplier,Institution,Contractor,Dealer,Consultant'),
    IsPrimary boolean OPTIONS (NAMEINSOURCE 'IsPrimary', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ReversePartnerId string(18) OPTIONS (NAMEINSOURCE 'ReversePartnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountToId FOREIGN KEY(AccountToId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'OpportunityPartnersTo'),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'OpportunityPartnersFrom')
) OPTIONS (NAMEINSOURCE 'OpportunityPartner', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpportunityShare (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserOrGroupId string(18) OPTIONS (NAMEINSOURCE 'UserOrGroupId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OpportunityAccessLevel string(40) OPTIONS (NAMEINSOURCE 'OpportunityAccessLevel', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Read,Edit,All'),
    RowCause string(40) OPTIONS (NAMEINSOURCE 'RowCause', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Owner,Manual,Rule,ImplicitChild,ImplicitParent,Team,Territory,TerritoryManual,TerritoryRule'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'Shares')
) OPTIONS (NAMEINSOURCE 'OpportunityShare', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OpportunityStage (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsClosed boolean OPTIONS (NAMEINSOURCE 'IsClosed', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsWon boolean OPTIONS (NAMEINSOURCE 'IsWon', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ForecastCategory string(40) OPTIONS (NAMEINSOURCE 'ForecastCategory', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Omitted,Pipeline,BestCase,Forecast,Closed'),
    ForecastCategoryName string(40) OPTIONS (NAMEINSOURCE 'ForecastCategoryName', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Omitted,Pipeline,Best Case,Commit,Closed'),
    DefaultProbability double OPTIONS (NAMEINSOURCE 'DefaultProbability', NATIVE_TYPE 'percent', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'OpportunityStage', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Organization (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Division string(80) OPTIONS (NAMEINSOURCE 'Division', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Street string(255) OPTIONS (NAMEINSOURCE 'Street', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    City string(40) OPTIONS (NAMEINSOURCE 'City', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    State string(80) OPTIONS (NAMEINSOURCE 'State', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PostalCode string(20) OPTIONS (NAMEINSOURCE 'PostalCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Country string(80) OPTIONS (NAMEINSOURCE 'Country', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Phone string(40) OPTIONS (NAMEINSOURCE 'Phone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Fax string(40) OPTIONS (NAMEINSOURCE 'Fax', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PrimaryContact string(80) OPTIONS (NAMEINSOURCE 'PrimaryContact', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DefaultLocaleSidKey string(40) OPTIONS (NAMEINSOURCE 'DefaultLocaleSidKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'sq_AL,ar_BH,ar_EG,ar_JO,ar_KW,ar_LB,ar_QA,ar_SA,ar_AE,hy_AM,az_AZ,eu_ES,be_BY,bn_BD,bs_BA,bg_BG,ca_ES,zh_CN,zh_HK,zh_MO,zh_SG,zh_TW,hr_HR,cs_CZ,da_DK,nl_BE,nl_NL,nl_SR,en_AU,en_BB,en_BM,en_CA,en_GH,en_IN,en_ID,en_IE,en_MY,en_NZ,en_NG,en_PK,en_PH,en_SG,en_ZA,en_GB,en_US,et_EE,fi_FI,fr_BE,fr_CA,fr_FR,fr_LU,fr_MC,fr_CH,ka_GE,de_AT,de_DE,de_LU,de_CH,el_GR,iw_IL,hi_IN,is_IS,ga_IE,it_IT,it_CH,ja_JP,kk_KZ,km_KH,ky_KG,ko_KR,lv_LV,lt_LT,mk_MK,ms_BN,ms_MY,mt_MT,sh_ME,no_NO,pt_AO,pt_BR,pt_PT,ro_MD,ro_RO,ru_RU,sr_BA,sh_BA,sh_CS,sr_CS,sk_SK,sl_SI,es_AR,es_BO,es_CL,es_CO,es_CR,es_DO,es_EC,es_SV,es_GT,es_HN,es_MX,es_PA,es_PY,es_PE,es_PR,es_ES,es_UY,es_VE,sv_SE,tl_PH,tg_TJ,th_TH,uk_UA,ur_PK,vi_VN,cy_GB'),
    LanguageLocaleKey string(40) OPTIONS (NAMEINSOURCE 'LanguageLocaleKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'en_US,de,es,fr,it,ja,sv,ko,zh_TW,zh_CN,pt_BR,nl_NL,da,th,fi,ru,es_MX'),
    ReceivesInfoEmails boolean OPTIONS (NAMEINSOURCE 'ReceivesInfoEmails', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ReceivesAdminInfoEmails boolean OPTIONS (NAMEINSOURCE 'ReceivesAdminInfoEmails', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    PreferencesRequireOpportunityProducts boolean OPTIONS (NAMEINSOURCE 'PreferencesRequireOpportunityProducts', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FiscalYearStartMonth integer OPTIONS (NAMEINSOURCE 'FiscalYearStartMonth', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UsesStartDateAsFiscalYearName boolean OPTIONS (NAMEINSOURCE 'UsesStartDateAsFiscalYearName', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    DefaultAccountAccess string(40) OPTIONS (NAMEINSOURCE 'DefaultAccountAccess', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    DefaultContactAccess string(40) OPTIONS (NAMEINSOURCE 'DefaultContactAccess', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit,ControlledByParent'),
    DefaultOpportunityAccess string(40) OPTIONS (NAMEINSOURCE 'DefaultOpportunityAccess', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    DefaultLeadAccess string(40) OPTIONS (NAMEINSOURCE 'DefaultLeadAccess', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit,ReadEditTransfer'),
    DefaultCaseAccess string(40) OPTIONS (NAMEINSOURCE 'DefaultCaseAccess', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit,ReadEditTransfer'),
    DefaultCalendarAccess string(40) OPTIONS (NAMEINSOURCE 'DefaultCalendarAccess', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'HideDetails,HideDetailsInsert,ShowDetails,ShowDetailsInsert,AllowEdits'),
    DefaultPricebookAccess string(40) OPTIONS (NAMEINSOURCE 'DefaultPricebookAccess', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,ReadSelect'),
    DefaultCampaignAccess string(40) OPTIONS (NAMEINSOURCE 'DefaultCampaignAccess', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit,All'),
    ComplianceBccEmail string(80) OPTIONS (NAMEINSOURCE 'ComplianceBccEmail', UPDATABLE FALSE, NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UiSkin string(40) OPTIONS (NAMEINSOURCE 'UiSkin', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Theme1,Theme2,PortalDefault,Webstore,Theme3'),
    TrialExpirationDate timestamp OPTIONS (NAMEINSOURCE 'TrialExpirationDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OrganizationType string(40) OPTIONS (NAMEINSOURCE 'OrganizationType', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Team Edition,Professional Edition,Enterprise Edition,Developer Edition,Personal Edition,Unlimited Edition,Contact Manager Edition,Base Edition'),
    WebToCaseDefaultOrigin string(40) OPTIONS (NAMEINSOURCE 'WebToCaseDefaultOrigin', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MonthlyPageViewsUsed integer OPTIONS (NAMEINSOURCE 'MonthlyPageViewsUsed', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MonthlyPageViewsEntitlement integer OPTIONS (NAMEINSOURCE 'MonthlyPageViewsEntitlement', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Organization', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE OrgWideEmailAddress (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Address string(270) OPTIONS (NAMEINSOURCE 'Address', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DisplayName string(300) OPTIONS (NAMEINSOURCE 'DisplayName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsAllowAllProfiles boolean OPTIONS (NAMEINSOURCE 'IsAllowAllProfiles', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'OrgWideEmailAddress', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Partner (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OpportunityId string(18) OPTIONS (NAMEINSOURCE 'OpportunityId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountFromId string(18) OPTIONS (NAMEINSOURCE 'AccountFromId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountToId string(18) OPTIONS (NAMEINSOURCE 'AccountToId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    "Role" string(40) OPTIONS (NAMEINSOURCE 'Role', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'System Integrator,Agency,Advertiser,VAR/Reseller,Distributor,Developer,Broker,Lender,Supplier,Institution,Contractor,Dealer,Consultant'),
    IsPrimary boolean OPTIONS (NAMEINSOURCE 'IsPrimary', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ReversePartnerId string(18) OPTIONS (NAMEINSOURCE 'ReversePartnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_AccountFromId FOREIGN KEY(AccountFromId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'PartnersFrom'),
    CONSTRAINT FK_Account_AccountToId FOREIGN KEY(AccountToId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'PartnersTo'),
    CONSTRAINT FK_Opportunity_OpportunityId FOREIGN KEY(OpportunityId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'Partners')
) OPTIONS (NAMEINSOURCE 'Partner', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE PartnerRole (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ReverseRole string(40) OPTIONS (NAMEINSOURCE 'ReverseRole', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'System Integrator,Agency,Advertiser,VAR/Reseller,Distributor,Developer,Broker,Lender,Supplier,Institution,Contractor,Dealer,Consultant'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'PartnerRole', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Period (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    FiscalYearSettingsId string(18) OPTIONS (NAMEINSOURCE 'FiscalYearSettingsId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Month,Quarter,Week,Year'),
    StartDate date OPTIONS (NAMEINSOURCE 'StartDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EndDate date OPTIONS (NAMEINSOURCE 'EndDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsForecastPeriod boolean OPTIONS (NAMEINSOURCE 'IsForecastPeriod', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    QuarterLabel string(40) OPTIONS (NAMEINSOURCE 'QuarterLabel', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Spring,Summer,Fall,Winter'),
    PeriodLabel string(40) OPTIONS (NAMEINSOURCE 'PeriodLabel', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" ''),
    Number integer OPTIONS (NAMEINSOURCE 'Number', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_FiscalYearSettings_FiscalYearSettingsId FOREIGN KEY(FiscalYearSettingsId) REFERENCES FiscalYearSettings (Id) OPTIONS (NAMEINSOURCE 'Periods')
) OPTIONS (NAMEINSOURCE 'Period', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE PermissionSet (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Label string(80) OPTIONS (NAMEINSOURCE 'Label', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserLicenseId string(18) OPTIONS (NAMEINSOURCE 'UserLicenseId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEmailSingle boolean OPTIONS (NAMEINSOURCE 'PermissionsEmailSingle', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEmailMass boolean OPTIONS (NAMEINSOURCE 'PermissionsEmailMass', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditTask boolean OPTIONS (NAMEINSOURCE 'PermissionsEditTask', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditEvent boolean OPTIONS (NAMEINSOURCE 'PermissionsEditEvent', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsExportReport boolean OPTIONS (NAMEINSOURCE 'PermissionsExportReport', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsImportPersonal boolean OPTIONS (NAMEINSOURCE 'PermissionsImportPersonal', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageUsers boolean OPTIONS (NAMEINSOURCE 'PermissionsManageUsers', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditPublicTemplates boolean OPTIONS (NAMEINSOURCE 'PermissionsEditPublicTemplates', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsModifyAllData boolean OPTIONS (NAMEINSOURCE 'PermissionsModifyAllData', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageCases boolean OPTIONS (NAMEINSOURCE 'PermissionsManageCases', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsMassInlineEdit boolean OPTIONS (NAMEINSOURCE 'PermissionsMassInlineEdit', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditKnowledge boolean OPTIONS (NAMEINSOURCE 'PermissionsEditKnowledge', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageKnowledge boolean OPTIONS (NAMEINSOURCE 'PermissionsManageKnowledge', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageSolutions boolean OPTIONS (NAMEINSOURCE 'PermissionsManageSolutions', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCustomizeApplication boolean OPTIONS (NAMEINSOURCE 'PermissionsCustomizeApplication', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditReadonlyFields boolean OPTIONS (NAMEINSOURCE 'PermissionsEditReadonlyFields', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsRunReports boolean OPTIONS (NAMEINSOURCE 'PermissionsRunReports', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewSetup boolean OPTIONS (NAMEINSOURCE 'PermissionsViewSetup', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsTransferAnyEntity boolean OPTIONS (NAMEINSOURCE 'PermissionsTransferAnyEntity', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsNewReportBuilder boolean OPTIONS (NAMEINSOURCE 'PermissionsNewReportBuilder', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsActivateContract boolean OPTIONS (NAMEINSOURCE 'PermissionsActivateContract', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsImportLeads boolean OPTIONS (NAMEINSOURCE 'PermissionsImportLeads', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageLeads boolean OPTIONS (NAMEINSOURCE 'PermissionsManageLeads', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsTransferAnyLead boolean OPTIONS (NAMEINSOURCE 'PermissionsTransferAnyLead', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewAllData boolean OPTIONS (NAMEINSOURCE 'PermissionsViewAllData', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditPublicDocuments boolean OPTIONS (NAMEINSOURCE 'PermissionsEditPublicDocuments', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditBrandTemplates boolean OPTIONS (NAMEINSOURCE 'PermissionsEditBrandTemplates', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditHtmlTemplates boolean OPTIONS (NAMEINSOURCE 'PermissionsEditHtmlTemplates', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageDashboards boolean OPTIONS (NAMEINSOURCE 'PermissionsManageDashboards', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsDeleteActivatedContract boolean OPTIONS (NAMEINSOURCE 'PermissionsDeleteActivatedContract', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsSendSitRequests boolean OPTIONS (NAMEINSOURCE 'PermissionsSendSitRequests', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageRemoteAccess boolean OPTIONS (NAMEINSOURCE 'PermissionsManageRemoteAccess', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCanUseNewDashboardBuilder boolean OPTIONS (NAMEINSOURCE 'PermissionsCanUseNewDashboardBuilder', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageCategories boolean OPTIONS (NAMEINSOURCE 'PermissionsManageCategories', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsConvertLeads boolean OPTIONS (NAMEINSOURCE 'PermissionsConvertLeads', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsPasswordNeverExpires boolean OPTIONS (NAMEINSOURCE 'PermissionsPasswordNeverExpires', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsUseTeamReassignWizards boolean OPTIONS (NAMEINSOURCE 'PermissionsUseTeamReassignWizards', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsInstallPackaging boolean OPTIONS (NAMEINSOURCE 'PermissionsInstallPackaging', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsPublishPackaging boolean OPTIONS (NAMEINSOURCE 'PermissionsPublishPackaging', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditOppLineItemUnitPrice boolean OPTIONS (NAMEINSOURCE 'PermissionsEditOppLineItemUnitPrice', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCreatePackaging boolean OPTIONS (NAMEINSOURCE 'PermissionsCreatePackaging', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsBulkApiHardDelete boolean OPTIONS (NAMEINSOURCE 'PermissionsBulkApiHardDelete', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsSolutionImport boolean OPTIONS (NAMEINSOURCE 'PermissionsSolutionImport', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageCallCenters boolean OPTIONS (NAMEINSOURCE 'PermissionsManageCallCenters', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditReports boolean OPTIONS (NAMEINSOURCE 'PermissionsEditReports', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageSynonyms boolean OPTIONS (NAMEINSOURCE 'PermissionsManageSynonyms', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewContent boolean OPTIONS (NAMEINSOURCE 'PermissionsViewContent', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageEmailClientConfig boolean OPTIONS (NAMEINSOURCE 'PermissionsManageEmailClientConfig', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEnableNotifications boolean OPTIONS (NAMEINSOURCE 'PermissionsEnableNotifications', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageDataIntegrations boolean OPTIONS (NAMEINSOURCE 'PermissionsManageDataIntegrations', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsDistributeFromPersWksp boolean OPTIONS (NAMEINSOURCE 'PermissionsDistributeFromPersWksp', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewDataCategories boolean OPTIONS (NAMEINSOURCE 'PermissionsViewDataCategories', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageDataCategories boolean OPTIONS (NAMEINSOURCE 'PermissionsManageDataCategories', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsAuthorApex boolean OPTIONS (NAMEINSOURCE 'PermissionsAuthorApex', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageMobile boolean OPTIONS (NAMEINSOURCE 'PermissionsManageMobile', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsApiEnabled boolean OPTIONS (NAMEINSOURCE 'PermissionsApiEnabled', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageCustomReportTypes boolean OPTIONS (NAMEINSOURCE 'PermissionsManageCustomReportTypes', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditCaseComments boolean OPTIONS (NAMEINSOURCE 'PermissionsEditCaseComments', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsTransferAnyCase boolean OPTIONS (NAMEINSOURCE 'PermissionsTransferAnyCase', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsContentAdministrator boolean OPTIONS (NAMEINSOURCE 'PermissionsContentAdministrator', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCreateWorkspaces boolean OPTIONS (NAMEINSOURCE 'PermissionsCreateWorkspaces', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageContentPermissions boolean OPTIONS (NAMEINSOURCE 'PermissionsManageContentPermissions', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageContentProperties boolean OPTIONS (NAMEINSOURCE 'PermissionsManageContentProperties', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageContentTypes boolean OPTIONS (NAMEINSOURCE 'PermissionsManageContentTypes', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageAnalyticSnapshots boolean OPTIONS (NAMEINSOURCE 'PermissionsManageAnalyticSnapshots', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsScheduleReports boolean OPTIONS (NAMEINSOURCE 'PermissionsScheduleReports', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageBusinessHourHolidays boolean OPTIONS (NAMEINSOURCE 'PermissionsManageBusinessHourHolidays', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageDynamicDashboards boolean OPTIONS (NAMEINSOURCE 'PermissionsManageDynamicDashboards', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCustomSidebarOnAllPages boolean OPTIONS (NAMEINSOURCE 'PermissionsCustomSidebarOnAllPages', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageInteraction boolean OPTIONS (NAMEINSOURCE 'PermissionsManageInteraction', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewMyTeamsDashboards boolean OPTIONS (NAMEINSOURCE 'PermissionsViewMyTeamsDashboards', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsModerateChatter boolean OPTIONS (NAMEINSOURCE 'PermissionsModerateChatter', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsResetPasswords boolean OPTIONS (NAMEINSOURCE 'PermissionsResetPasswords', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsFlowUFLRequired boolean OPTIONS (NAMEINSOURCE 'PermissionsFlowUFLRequired', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCanInsertFeedSystemFields boolean OPTIONS (NAMEINSOURCE 'PermissionsCanInsertFeedSystemFields', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageKnowledgeImportExport boolean OPTIONS (NAMEINSOURCE 'PermissionsManageKnowledgeImportExport', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEmailTemplateManagement boolean OPTIONS (NAMEINSOURCE 'PermissionsEmailTemplateManagement', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEmailAdministration boolean OPTIONS (NAMEINSOURCE 'PermissionsEmailAdministration', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageNetworks boolean OPTIONS (NAMEINSOURCE 'PermissionsManageNetworks', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'PermissionSet', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE PermissionSetAssignment (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    PermissionSetId string(18) OPTIONS (NAMEINSOURCE 'PermissionSetId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AssigneeId string(18) OPTIONS (NAMEINSOURCE 'AssigneeId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_PermissionSet_PermissionSetId FOREIGN KEY(PermissionSetId) REFERENCES PermissionSet (Id) OPTIONS (NAMEINSOURCE 'Assignments'),
    CONSTRAINT FK_User__AssigneeId FOREIGN KEY(AssigneeId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'PermissionSetAssignments')
) OPTIONS (NAMEINSOURCE 'PermissionSetAssignment', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Pricebook2 (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsStandard boolean OPTIONS (NAMEINSOURCE 'IsStandard', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Pricebook2', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Pricebook2History (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Pricebook2Id string(18) OPTIONS (NAMEINSOURCE 'Pricebook2Id', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'created,Description,feedEvent,IsActive,IsArchived,IsStandard,Name,ownerAccepted,ownerAssignment,locked,unlocked'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Pricebook2_Pricebook2Id FOREIGN KEY(Pricebook2Id) REFERENCES Pricebook2 (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'Pricebook2History', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE PricebookEntry (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Pricebook2Id string(18) OPTIONS (NAMEINSOURCE 'Pricebook2Id', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Product2Id string(18) OPTIONS (NAMEINSOURCE 'Product2Id', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UnitPrice double OPTIONS (NAMEINSOURCE 'UnitPrice', CURRENCY TRUE, NATIVE_TYPE 'currency', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    UseStandardPrice boolean OPTIONS (NAMEINSOURCE 'UseStandardPrice', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ProductCode string(255) OPTIONS (NAMEINSOURCE 'ProductCode', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Pricebook2_Pricebook2Id FOREIGN KEY(Pricebook2Id) REFERENCES Pricebook2 (Id) OPTIONS (NAMEINSOURCE 'PricebookEntries'),
    CONSTRAINT FK_Product2_Product2Id FOREIGN KEY(Product2Id) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'PricebookEntries')
) OPTIONS (NAMEINSOURCE 'PricebookEntry', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ProcessDefinition (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(40) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Approval,State'),
    Description string(1000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TableEnumOrId string(40) OPTIONS (NAMEINSOURCE 'TableEnumOrId', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,Asset,Campaign,Case,Contact,Contract,KnowledgeArticle,KnowledgeArticleVersion,Lead,Opportunity,Product2,Solution'),
    LockType string(40) OPTIONS (NAMEINSOURCE 'LockType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Total,Admin,Owner,Workitem,Node,none'),
    State string(40) OPTIONS (NAMEINSOURCE 'State', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Active,Inactive,Obsolete'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ProcessDefinition', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ProcessInstance (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ProcessDefinitionId string(18) OPTIONS (NAMEINSOURCE 'ProcessDefinitionId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TargetObjectId string(18) OPTIONS (NAMEINSOURCE 'TargetObjectId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Approved,Rejected,Removed,Fault,Pending,Held,Reassigned,Started,NoResponse'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Asset_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Campaign_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Case__TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Contact_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Contract_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Lead_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Opportunity_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Product2_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances'),
    CONSTRAINT FK_Solution_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'ProcessInstances')
) OPTIONS (NAMEINSOURCE 'ProcessInstance', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ProcessInstanceHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsPending boolean OPTIONS (NAMEINSOURCE 'IsPending', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ProcessInstanceId string(18) OPTIONS (NAMEINSOURCE 'ProcessInstanceId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    TargetObjectId string(18) OPTIONS (NAMEINSOURCE 'TargetObjectId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StepStatus string(40) OPTIONS (NAMEINSOURCE 'StepStatus', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Approved,Rejected,Removed,Fault,Pending,Held,Reassigned,Started,NoResponse'),
    OriginalActorId string(18) OPTIONS (NAMEINSOURCE 'OriginalActorId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ActorId string(18) OPTIONS (NAMEINSOURCE 'ActorId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RemindersSent integer OPTIONS (NAMEINSOURCE 'RemindersSent', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Comments string OPTIONS (NAMEINSOURCE 'Comments', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_Asset_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_Campaign_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_Case__TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_Contact_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_Contract_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_Lead_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_Opportunity_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_ProcessInstance_ProcessInstanceId FOREIGN KEY(ProcessInstanceId) REFERENCES ProcessInstance (Id) OPTIONS (NAMEINSOURCE 'StepsAndWorkitems'),
    CONSTRAINT FK_Product2_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps'),
    CONSTRAINT FK_Solution_TargetObjectId FOREIGN KEY(TargetObjectId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'ProcessSteps')
) OPTIONS (NAMEINSOURCE 'ProcessInstanceHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'false', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'false', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ProcessInstanceStep (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ProcessInstanceId string(18) OPTIONS (NAMEINSOURCE 'ProcessInstanceId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    StepStatus string(40) OPTIONS (NAMEINSOURCE 'StepStatus', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Approved,Rejected,Removed,Fault,Pending,Held,Reassigned,Started,NoResponse'),
    OriginalActorId string(18) OPTIONS (NAMEINSOURCE 'OriginalActorId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ActorId string(18) OPTIONS (NAMEINSOURCE 'ActorId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Comments string OPTIONS (NAMEINSOURCE 'Comments', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_ProcessInstance_ProcessInstanceId FOREIGN KEY(ProcessInstanceId) REFERENCES ProcessInstance (Id) OPTIONS (NAMEINSOURCE 'Steps')
) OPTIONS (NAMEINSOURCE 'ProcessInstanceStep', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ProcessInstanceWorkitem (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ProcessInstanceId string(18) OPTIONS (NAMEINSOURCE 'ProcessInstanceId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OriginalActorId string(18) OPTIONS (NAMEINSOURCE 'OriginalActorId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ActorId string(18) OPTIONS (NAMEINSOURCE 'ActorId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_ProcessInstance_ProcessInstanceId FOREIGN KEY(ProcessInstanceId) REFERENCES ProcessInstance (Id) OPTIONS (NAMEINSOURCE 'Workitems')
) OPTIONS (NAMEINSOURCE 'ProcessInstanceWorkitem', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE ProcessNode (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ProcessDefinitionId string(18) OPTIONS (NAMEINSOURCE 'ProcessDefinitionId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(1000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'ProcessNode', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Product2 (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ProductCode string(255) OPTIONS (NAMEINSOURCE 'ProductCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Family string(40) OPTIONS (NAMEINSOURCE 'Family', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" ''),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Product2', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE Product2Feed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Product2_ParentId FOREIGN KEY(ParentId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'Product2Feed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Profile (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEmailSingle boolean OPTIONS (NAMEINSOURCE 'PermissionsEmailSingle', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEmailMass boolean OPTIONS (NAMEINSOURCE 'PermissionsEmailMass', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditTask boolean OPTIONS (NAMEINSOURCE 'PermissionsEditTask', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditEvent boolean OPTIONS (NAMEINSOURCE 'PermissionsEditEvent', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsExportReport boolean OPTIONS (NAMEINSOURCE 'PermissionsExportReport', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsImportPersonal boolean OPTIONS (NAMEINSOURCE 'PermissionsImportPersonal', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageUsers boolean OPTIONS (NAMEINSOURCE 'PermissionsManageUsers', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditPublicTemplates boolean OPTIONS (NAMEINSOURCE 'PermissionsEditPublicTemplates', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsModifyAllData boolean OPTIONS (NAMEINSOURCE 'PermissionsModifyAllData', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageCases boolean OPTIONS (NAMEINSOURCE 'PermissionsManageCases', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsMassInlineEdit boolean OPTIONS (NAMEINSOURCE 'PermissionsMassInlineEdit', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditKnowledge boolean OPTIONS (NAMEINSOURCE 'PermissionsEditKnowledge', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageKnowledge boolean OPTIONS (NAMEINSOURCE 'PermissionsManageKnowledge', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageSolutions boolean OPTIONS (NAMEINSOURCE 'PermissionsManageSolutions', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCustomizeApplication boolean OPTIONS (NAMEINSOURCE 'PermissionsCustomizeApplication', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditReadonlyFields boolean OPTIONS (NAMEINSOURCE 'PermissionsEditReadonlyFields', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsRunReports boolean OPTIONS (NAMEINSOURCE 'PermissionsRunReports', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewSetup boolean OPTIONS (NAMEINSOURCE 'PermissionsViewSetup', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsTransferAnyEntity boolean OPTIONS (NAMEINSOURCE 'PermissionsTransferAnyEntity', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsNewReportBuilder boolean OPTIONS (NAMEINSOURCE 'PermissionsNewReportBuilder', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsActivateContract boolean OPTIONS (NAMEINSOURCE 'PermissionsActivateContract', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsImportLeads boolean OPTIONS (NAMEINSOURCE 'PermissionsImportLeads', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageLeads boolean OPTIONS (NAMEINSOURCE 'PermissionsManageLeads', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsTransferAnyLead boolean OPTIONS (NAMEINSOURCE 'PermissionsTransferAnyLead', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewAllData boolean OPTIONS (NAMEINSOURCE 'PermissionsViewAllData', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditPublicDocuments boolean OPTIONS (NAMEINSOURCE 'PermissionsEditPublicDocuments', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditBrandTemplates boolean OPTIONS (NAMEINSOURCE 'PermissionsEditBrandTemplates', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditHtmlTemplates boolean OPTIONS (NAMEINSOURCE 'PermissionsEditHtmlTemplates', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageDashboards boolean OPTIONS (NAMEINSOURCE 'PermissionsManageDashboards', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsDeleteActivatedContract boolean OPTIONS (NAMEINSOURCE 'PermissionsDeleteActivatedContract', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsSendSitRequests boolean OPTIONS (NAMEINSOURCE 'PermissionsSendSitRequests', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageRemoteAccess boolean OPTIONS (NAMEINSOURCE 'PermissionsManageRemoteAccess', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCanUseNewDashboardBuilder boolean OPTIONS (NAMEINSOURCE 'PermissionsCanUseNewDashboardBuilder', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageCategories boolean OPTIONS (NAMEINSOURCE 'PermissionsManageCategories', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsConvertLeads boolean OPTIONS (NAMEINSOURCE 'PermissionsConvertLeads', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsPasswordNeverExpires boolean OPTIONS (NAMEINSOURCE 'PermissionsPasswordNeverExpires', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsUseTeamReassignWizards boolean OPTIONS (NAMEINSOURCE 'PermissionsUseTeamReassignWizards', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsInstallMultiforce boolean OPTIONS (NAMEINSOURCE 'PermissionsInstallMultiforce', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsPublishMultiforce boolean OPTIONS (NAMEINSOURCE 'PermissionsPublishMultiforce', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditOppLineItemUnitPrice boolean OPTIONS (NAMEINSOURCE 'PermissionsEditOppLineItemUnitPrice', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCreateMultiforce boolean OPTIONS (NAMEINSOURCE 'PermissionsCreateMultiforce', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsBulkApiHardDelete boolean OPTIONS (NAMEINSOURCE 'PermissionsBulkApiHardDelete', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsSolutionImport boolean OPTIONS (NAMEINSOURCE 'PermissionsSolutionImport', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageCallCenters boolean OPTIONS (NAMEINSOURCE 'PermissionsManageCallCenters', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditReports boolean OPTIONS (NAMEINSOURCE 'PermissionsEditReports', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageSynonyms boolean OPTIONS (NAMEINSOURCE 'PermissionsManageSynonyms', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewContent boolean OPTIONS (NAMEINSOURCE 'PermissionsViewContent', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageEmailClientConfig boolean OPTIONS (NAMEINSOURCE 'PermissionsManageEmailClientConfig', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEnableNotifications boolean OPTIONS (NAMEINSOURCE 'PermissionsEnableNotifications', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageDataIntegrations boolean OPTIONS (NAMEINSOURCE 'PermissionsManageDataIntegrations', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsDistributeFromPersWksp boolean OPTIONS (NAMEINSOURCE 'PermissionsDistributeFromPersWksp', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewDataCategories boolean OPTIONS (NAMEINSOURCE 'PermissionsViewDataCategories', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageDataCategories boolean OPTIONS (NAMEINSOURCE 'PermissionsManageDataCategories', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsAuthorApex boolean OPTIONS (NAMEINSOURCE 'PermissionsAuthorApex', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageMobile boolean OPTIONS (NAMEINSOURCE 'PermissionsManageMobile', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsApiEnabled boolean OPTIONS (NAMEINSOURCE 'PermissionsApiEnabled', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageCustomReportTypes boolean OPTIONS (NAMEINSOURCE 'PermissionsManageCustomReportTypes', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEditCaseComments boolean OPTIONS (NAMEINSOURCE 'PermissionsEditCaseComments', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsTransferAnyCase boolean OPTIONS (NAMEINSOURCE 'PermissionsTransferAnyCase', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsContentAdministrator boolean OPTIONS (NAMEINSOURCE 'PermissionsContentAdministrator', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCreateWorkspaces boolean OPTIONS (NAMEINSOURCE 'PermissionsCreateWorkspaces', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageContentPermissions boolean OPTIONS (NAMEINSOURCE 'PermissionsManageContentPermissions', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageContentProperties boolean OPTIONS (NAMEINSOURCE 'PermissionsManageContentProperties', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageContentTypes boolean OPTIONS (NAMEINSOURCE 'PermissionsManageContentTypes', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageAnalyticSnapshots boolean OPTIONS (NAMEINSOURCE 'PermissionsManageAnalyticSnapshots', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsScheduleReports boolean OPTIONS (NAMEINSOURCE 'PermissionsScheduleReports', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageBusinessHourHolidays boolean OPTIONS (NAMEINSOURCE 'PermissionsManageBusinessHourHolidays', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageDynamicDashboards boolean OPTIONS (NAMEINSOURCE 'PermissionsManageDynamicDashboards', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCustomSidebarOnAllPages boolean OPTIONS (NAMEINSOURCE 'PermissionsCustomSidebarOnAllPages', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageInteraction boolean OPTIONS (NAMEINSOURCE 'PermissionsManageInteraction', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsViewMyTeamsDashboards boolean OPTIONS (NAMEINSOURCE 'PermissionsViewMyTeamsDashboards', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsModerateChatter boolean OPTIONS (NAMEINSOURCE 'PermissionsModerateChatter', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsResetPasswords boolean OPTIONS (NAMEINSOURCE 'PermissionsResetPasswords', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsFlowUFLRequired boolean OPTIONS (NAMEINSOURCE 'PermissionsFlowUFLRequired', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsCanInsertFeedSystemFields boolean OPTIONS (NAMEINSOURCE 'PermissionsCanInsertFeedSystemFields', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageKnowledgeImportExport boolean OPTIONS (NAMEINSOURCE 'PermissionsManageKnowledgeImportExport', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEmailTemplateManagement boolean OPTIONS (NAMEINSOURCE 'PermissionsEmailTemplateManagement', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsEmailAdministration boolean OPTIONS (NAMEINSOURCE 'PermissionsEmailAdministration', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PermissionsManageNetworks boolean OPTIONS (NAMEINSOURCE 'PermissionsManageNetworks', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserLicenseId string(18) OPTIONS (NAMEINSOURCE 'UserLicenseId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserType string(40) OPTIONS (NAMEINSOURCE 'UserType', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Standard,PowerPartner,PowerCustomerSuccess,CustomerSuccess,Guest,CSPLitePortal,CSNOnly,SelfService'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Profile', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE PushTopic (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(25) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Query string(1300) OPTIONS (NAMEINSOURCE 'Query', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ApiVersion double OPTIONS (NAMEINSOURCE 'ApiVersion', NATIVE_TYPE 'double', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(400) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'PushTopic', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE QueueSobject (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    QueueId string(18) OPTIONS (NAMEINSOURCE 'QueueId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SobjectType string(40) OPTIONS (NAMEINSOURCE 'SobjectType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Case,Lead'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Group__QueueId FOREIGN KEY(QueueId) REFERENCES Group_ (Id) OPTIONS (NAMEINSOURCE 'QueueSobjects')
) OPTIONS (NAMEINSOURCE 'QueueSobject', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE RecordType (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BusinessProcessId string(18) OPTIONS (NAMEINSOURCE 'BusinessProcessId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SobjectType string(40) OPTIONS (NAMEINSOURCE 'SobjectType', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,Announcement,Campaign,CampaignMember,Case,CollaborationFolder,CollaborationFolderMember,Contact,ContentVersion,Contract,Event,Idea,InboundSocialPost,Lead,Opportunity,Pricebook2,Product2,Solution,Task'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'RecordType', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Report (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(40) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DeveloperName string(80) OPTIONS (NAMEINSOURCE 'DeveloperName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastRunDate timestamp OPTIONS (NAMEINSOURCE 'LastRunDate', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Report', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE ReportFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Report_ParentId FOREIGN KEY(ParentId) REFERENCES Report (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'ReportFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Site (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Subdomain string(80) OPTIONS (NAMEINSOURCE 'Subdomain', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UrlPathPrefix string(40) OPTIONS (NAMEINSOURCE 'UrlPathPrefix', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Active,Inactive'),
    AdminId string(18) OPTIONS (NAMEINSOURCE 'AdminId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OptionsEnableFeeds boolean OPTIONS (NAMEINSOURCE 'OptionsEnableFeeds', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OptionsAllowHomePage boolean OPTIONS (NAMEINSOURCE 'OptionsAllowHomePage', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OptionsAllowStandardIdeasPages boolean OPTIONS (NAMEINSOURCE 'OptionsAllowStandardIdeasPages', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OptionsAllowStandardSearch boolean OPTIONS (NAMEINSOURCE 'OptionsAllowStandardSearch', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OptionsAllowStandardLookups boolean OPTIONS (NAMEINSOURCE 'OptionsAllowStandardLookups', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OptionsAllowStandardAnswersPages boolean OPTIONS (NAMEINSOURCE 'OptionsAllowStandardAnswersPages', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(1000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AnalyticsTrackingCode string(40) OPTIONS (NAMEINSOURCE 'AnalyticsTrackingCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SiteType string(40) OPTIONS (NAMEINSOURCE 'SiteType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Siteforce,Visualforce,User'),
    DailyBandwidthLimit integer OPTIONS (NAMEINSOURCE 'DailyBandwidthLimit', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DailyBandwidthUsed integer OPTIONS (NAMEINSOURCE 'DailyBandwidthUsed', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DailyRequestTimeLimit integer OPTIONS (NAMEINSOURCE 'DailyRequestTimeLimit', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DailyRequestTimeUsed integer OPTIONS (NAMEINSOURCE 'DailyRequestTimeUsed', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MonthlyPageViewsEntitlement integer OPTIONS (NAMEINSOURCE 'MonthlyPageViewsEntitlement', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_User__AdminId FOREIGN KEY(AdminId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'UserSites')
) OPTIONS (NAMEINSOURCE 'Site', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE SiteDomain (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    SiteId string(18) OPTIONS (NAMEINSOURCE 'SiteId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Domain string(765) OPTIONS (NAMEINSOURCE 'Domain', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Site_SiteId FOREIGN KEY(SiteId) REFERENCES Site (Id) OPTIONS (NAMEINSOURCE 'SiteDomains')
) OPTIONS (NAMEINSOURCE 'SiteDomain', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE SiteFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Site_ParentId FOREIGN KEY(ParentId) REFERENCES Site (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'SiteFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE SiteHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    SiteId string(18) OPTIONS (NAMEINSOURCE 'SiteId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'siteActive,Admin,AnalyticsTrackingCode,siteOverride401,siteOverrideChangePassword,ClickjackProtectionLevel,created,SiteDeleteDomain,siteNewDomain,DefaultDomain,siteSetPrimaryDomain,Description,siteBTDisabled,siteEnableFeeds,siteAllowStandardAnswersPages,siteAllowHomePage,siteAllowStandardIdeasPages,siteAllowStandardSearch,siteAllowStandardLookups,FavoriteIcon,feedEvent,siteOverride500,GuestUser,Guid,siteOverrideInactive,IndexPage,Language,siteOverride509,siteOverride503,MasterLabel,sitePageLimitExceeded,siteOverrideMyProfile,Name,NewPassTemplate,NewUserTemplate,Options,ownerAccepted,ownerAssignment,siteOverride404,Portal,locked,unlocked,siteNewRedirect,siteDeleteRedirect,siteChangeRedirect,siteRequireInsecurePortalAccess,ServerIsDown,siteOverrideRobotsTxt,siteOverrideTemplate,SiteType,Status,Subdomain,TopLevelDomain,UrlPathPrefix,UrlRewriterClass'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Site_SiteId FOREIGN KEY(SiteId) REFERENCES Site (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'SiteHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Solution (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    SolutionNumber string(30) OPTIONS (NAMEINSOURCE 'SolutionNumber', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    SolutionName string(255) OPTIONS (NAMEINSOURCE 'SolutionName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsPublished boolean OPTIONS (NAMEINSOURCE 'IsPublished', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsPublishedInPublicKb boolean OPTIONS (NAMEINSOURCE 'IsPublishedInPublicKb', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Draft,Reviewed,Duplicate'),
    IsReviewed boolean OPTIONS (NAMEINSOURCE 'IsReviewed', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    SolutionNote string(32000) OPTIONS (NAMEINSOURCE 'SolutionNote', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    TimesUsed integer OPTIONS (NAMEINSOURCE 'TimesUsed', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsHtml boolean OPTIONS (NAMEINSOURCE 'IsHtml', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Solution', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE SolutionFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Solution_ParentId FOREIGN KEY(ParentId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'SolutionFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE SolutionHistory (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    SolutionId string(18) OPTIONS (NAMEINSOURCE 'SolutionId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Field string(255) OPTIONS (NAMEINSOURCE 'Field', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'created,feedEvent,IsPublished,IsPublishedInPublicKb,ownerAccepted,ownerAssignment,locked,unlocked,SolutionName,SolutionNote,Status'),
    OldValue string(255) OPTIONS (NAMEINSOURCE 'OldValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    NewValue string(255) OPTIONS (NAMEINSOURCE 'NewValue', NATIVE_TYPE 'anyType', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Solution_SolutionId FOREIGN KEY(SolutionId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'Histories')
) OPTIONS (NAMEINSOURCE 'SolutionHistory', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE SolutionStatus (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDefault boolean OPTIONS (NAMEINSOURCE 'IsDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsReviewed boolean OPTIONS (NAMEINSOURCE 'IsReviewed', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'SolutionStatus', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE StaticResource (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(255) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    BodyLength integer OPTIONS (NAMEINSOURCE 'BodyLength', UPDATABLE FALSE, NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body blob OPTIONS (NAMEINSOURCE 'Body', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(255) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CacheControl string(40) OPTIONS (NAMEINSOURCE 'CacheControl', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Private,Public'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'StaticResource', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Task (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    WhoId string(18) OPTIONS (NAMEINSOURCE 'WhoId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    WhatId string(18) OPTIONS (NAMEINSOURCE 'WhatId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Subject string(255) OPTIONS (NAMEINSOURCE 'Subject', NATIVE_TYPE 'combobox', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ActivityDate date OPTIONS (NAMEINSOURCE 'ActivityDate', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Status string(40) OPTIONS (NAMEINSOURCE 'Status', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Not Started,In Progress,Completed,Waiting on someone else,Deferred'),
    Priority string(40) OPTIONS (NAMEINSOURCE 'Priority', NATIVE_TYPE 'picklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'High,Normal,Low'),
    OwnerId string(18) OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Description string(32000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsClosed boolean OPTIONS (NAMEINSOURCE 'IsClosed', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsArchived boolean OPTIONS (NAMEINSOURCE 'IsArchived', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CallDurationInSeconds integer OPTIONS (NAMEINSOURCE 'CallDurationInSeconds', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CallType string(40) OPTIONS (NAMEINSOURCE 'CallType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Internal,Inbound,Outbound'),
    CallDisposition string(255) OPTIONS (NAMEINSOURCE 'CallDisposition', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CallObject string(255) OPTIONS (NAMEINSOURCE 'CallObject', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ReminderDateTime timestamp OPTIONS (NAMEINSOURCE 'ReminderDateTime', NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsReminderSet boolean OPTIONS (NAMEINSOURCE 'IsReminderSet', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    RecurrenceActivityId string(18) OPTIONS (NAMEINSOURCE 'RecurrenceActivityId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsRecurrence boolean OPTIONS (NAMEINSOURCE 'IsRecurrence', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    RecurrenceStartDateOnly date OPTIONS (NAMEINSOURCE 'RecurrenceStartDateOnly', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceEndDateOnly date OPTIONS (NAMEINSOURCE 'RecurrenceEndDateOnly', NATIVE_TYPE 'date', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceTimeZoneSidKey string(40) OPTIONS (NAMEINSOURCE 'RecurrenceTimeZoneSidKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Pacific/Kiritimati,Pacific/Enderbury,Pacific/Tongatapu,Pacific/Chatham,Asia/Kamchatka,Pacific/Auckland,Pacific/Fiji,Pacific/Norfolk,Pacific/Guadalcanal,Australia/Lord_Howe,Australia/Brisbane,Australia/Sydney,Australia/Adelaide,Australia/Darwin,Asia/Seoul,Asia/Tokyo,Asia/Hong_Kong,Asia/Kuala_Lumpur,Asia/Manila,Asia/Shanghai,Asia/Singapore,Asia/Taipei,Australia/Perth,Asia/Bangkok,Asia/Ho_Chi_Minh,Asia/Jakarta,Asia/Rangoon,Asia/Dhaka,Asia/Yekaterinburg,Asia/Kathmandu,Asia/Colombo,Asia/Kolkata,Asia/Karachi,Asia/Tashkent,Asia/Kabul,Asia/Tehran,Asia/Dubai,Asia/Tbilisi,Europe/Moscow,Africa/Nairobi,Asia/Baghdad,Asia/Jerusalem,Asia/Kuwait,Asia/Riyadh,Europe/Athens,Europe/Bucharest,Europe/Helsinki,Europe/Istanbul,Europe/Minsk,Africa/Cairo,Africa/Johannesburg,Europe/Amsterdam,Europe/Berlin,Europe/Brussels,Europe/Paris,Europe/Prague,Europe/Rome,Africa/Algiers,Europe/Dublin,Europe/Lisbon,Europe/London,GMT,Atlantic/Cape_Verde,Atlantic/South_Georgia,America/St_Johns,America/Argentina/Buenos_Aires,America/Halifax,America/Sao_Paulo,Atlantic/Bermuda,America/Indiana/Indianapolis,America/New_York,America/Puerto_Rico,America/Santiago,America/Caracas,America/Bogota,America/Chicago,America/Lima,America/Mexico_City,America/Panama,America/Denver,America/El_Salvador,America/Los_Angeles,America/Phoenix,America/Tijuana,America/Anchorage,Pacific/Honolulu,Pacific/Niue,Pacific/Pago_Pago'),
    RecurrenceType string(40) OPTIONS (NAMEINSOURCE 'RecurrenceType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'RecursDaily,RecursEveryWeekday,RecursMonthly,RecursMonthlyNth,RecursWeekly,RecursYearly,RecursYearlyNth'),
    RecurrenceInterval integer OPTIONS (NAMEINSOURCE 'RecurrenceInterval', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceDayOfWeekMask integer OPTIONS (NAMEINSOURCE 'RecurrenceDayOfWeekMask', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceDayOfMonth integer OPTIONS (NAMEINSOURCE 'RecurrenceDayOfMonth', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RecurrenceInstance string(40) OPTIONS (NAMEINSOURCE 'RecurrenceInstance', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'First,Second,Third,Fourth,Last'),
    RecurrenceMonthOfYear string(40) OPTIONS (NAMEINSOURCE 'RecurrenceMonthOfYear', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'January,February,March,April,May,June,July,August,September,October,November,December'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Account_WhatId FOREIGN KEY(WhatId) REFERENCES Account (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Asset_WhatId FOREIGN KEY(WhatId) REFERENCES Asset (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Campaign_WhatId FOREIGN KEY(WhatId) REFERENCES Campaign (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Case__WhatId FOREIGN KEY(WhatId) REFERENCES Case_ (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Contact_WhoId FOREIGN KEY(WhoId) REFERENCES Contact (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Contract_WhatId FOREIGN KEY(WhatId) REFERENCES Contract (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Lead_WhoId FOREIGN KEY(WhoId) REFERENCES Lead (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Opportunity_WhatId FOREIGN KEY(WhatId) REFERENCES Opportunity (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Product2_WhatId FOREIGN KEY(WhatId) REFERENCES Product2 (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Solution_WhatId FOREIGN KEY(WhatId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'Tasks'),
    CONSTRAINT FK_Task_RecurrenceActivityId FOREIGN KEY(RecurrenceActivityId) REFERENCES Task (Id) OPTIONS (NAMEINSOURCE 'RecurringTasks')
) OPTIONS (NAMEINSOURCE 'Task', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE TaskFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Task_ParentId FOREIGN KEY(ParentId) REFERENCES Task (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'TaskFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE TaskPriority (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDefault boolean OPTIONS (NAMEINSOURCE 'IsDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsHighPriority boolean OPTIONS (NAMEINSOURCE 'IsHighPriority', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'TaskPriority', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE TaskStatus (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    MasterLabel string(255) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SortOrder integer OPTIONS (NAMEINSOURCE 'SortOrder', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsDefault boolean OPTIONS (NAMEINSOURCE 'IsDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsClosed boolean OPTIONS (NAMEINSOURCE 'IsClosed', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'TaskStatus', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE User_ (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Username string(80) OPTIONS (NAMEINSOURCE 'Username', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastName string(80) OPTIONS (NAMEINSOURCE 'LastName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FirstName string(40) OPTIONS (NAMEINSOURCE 'FirstName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(121) OPTIONS (NAMEINSOURCE 'Name', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CompanyName string(80) OPTIONS (NAMEINSOURCE 'CompanyName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Division string(80) OPTIONS (NAMEINSOURCE 'Division', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Department string(80) OPTIONS (NAMEINSOURCE 'Department', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(80) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Street string(255) OPTIONS (NAMEINSOURCE 'Street', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    City string(40) OPTIONS (NAMEINSOURCE 'City', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    State string(80) OPTIONS (NAMEINSOURCE 'State', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PostalCode string(20) OPTIONS (NAMEINSOURCE 'PostalCode', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Country string(80) OPTIONS (NAMEINSOURCE 'Country', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Email string(128) OPTIONS (NAMEINSOURCE 'Email', NATIVE_TYPE 'email', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Phone string(40) OPTIONS (NAMEINSOURCE 'Phone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Fax string(40) OPTIONS (NAMEINSOURCE 'Fax', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MobilePhone string(40) OPTIONS (NAMEINSOURCE 'MobilePhone', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Alias string(8) OPTIONS (NAMEINSOURCE 'Alias', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CommunityNickname string(40) OPTIONS (NAMEINSOURCE 'CommunityNickname', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsActive boolean OPTIONS (NAMEINSOURCE 'IsActive', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    TimeZoneSidKey string(40) OPTIONS (NAMEINSOURCE 'TimeZoneSidKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Pacific/Kiritimati,Pacific/Enderbury,Pacific/Tongatapu,Pacific/Chatham,Asia/Kamchatka,Pacific/Auckland,Pacific/Fiji,Pacific/Norfolk,Pacific/Guadalcanal,Australia/Lord_Howe,Australia/Brisbane,Australia/Sydney,Australia/Adelaide,Australia/Darwin,Asia/Seoul,Asia/Tokyo,Asia/Hong_Kong,Asia/Kuala_Lumpur,Asia/Manila,Asia/Shanghai,Asia/Singapore,Asia/Taipei,Australia/Perth,Asia/Bangkok,Asia/Ho_Chi_Minh,Asia/Jakarta,Asia/Rangoon,Asia/Dhaka,Asia/Yekaterinburg,Asia/Kathmandu,Asia/Colombo,Asia/Kolkata,Asia/Karachi,Asia/Tashkent,Asia/Kabul,Asia/Tehran,Asia/Dubai,Asia/Tbilisi,Europe/Moscow,Africa/Nairobi,Asia/Baghdad,Asia/Jerusalem,Asia/Kuwait,Asia/Riyadh,Europe/Athens,Europe/Bucharest,Europe/Helsinki,Europe/Istanbul,Europe/Minsk,Africa/Cairo,Africa/Johannesburg,Europe/Amsterdam,Europe/Berlin,Europe/Brussels,Europe/Paris,Europe/Prague,Europe/Rome,Africa/Algiers,Europe/Dublin,Europe/Lisbon,Europe/London,GMT,Atlantic/Cape_Verde,Atlantic/South_Georgia,America/St_Johns,America/Argentina/Buenos_Aires,America/Halifax,America/Sao_Paulo,Atlantic/Bermuda,America/Indiana/Indianapolis,America/New_York,America/Puerto_Rico,America/Santiago,America/Caracas,America/Bogota,America/Chicago,America/Lima,America/Mexico_City,America/Panama,America/Denver,America/El_Salvador,America/Los_Angeles,America/Phoenix,America/Tijuana,America/Anchorage,Pacific/Honolulu,Pacific/Niue,Pacific/Pago_Pago'),
    UserRoleId string(18) OPTIONS (NAMEINSOURCE 'UserRoleId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LocaleSidKey string(40) OPTIONS (NAMEINSOURCE 'LocaleSidKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'sq_AL,ar_BH,ar_EG,ar_JO,ar_KW,ar_LB,ar_QA,ar_SA,ar_AE,hy_AM,az_AZ,eu_ES,be_BY,bn_BD,bs_BA,bg_BG,ca_ES,zh_CN,zh_HK,zh_MO,zh_SG,zh_TW,hr_HR,cs_CZ,da_DK,nl_BE,nl_NL,nl_SR,en_AU,en_BB,en_BM,en_CA,en_GH,en_IN,en_ID,en_IE,en_MY,en_NZ,en_NG,en_PK,en_PH,en_SG,en_ZA,en_GB,en_US,et_EE,fi_FI,fr_BE,fr_CA,fr_FR,fr_LU,fr_MC,fr_CH,ka_GE,de_AT,de_DE,de_LU,de_CH,el_GR,iw_IL,hi_IN,is_IS,ga_IE,it_IT,it_CH,ja_JP,kk_KZ,km_KH,ky_KG,ko_KR,lv_LV,lt_LT,mk_MK,ms_BN,ms_MY,mt_MT,sh_ME,no_NO,pt_AO,pt_BR,pt_PT,ro_MD,ro_RO,ru_RU,sr_BA,sh_BA,sh_CS,sr_CS,sk_SK,sl_SI,es_AR,es_BO,es_CL,es_CO,es_CR,es_DO,es_EC,es_SV,es_GT,es_HN,es_MX,es_PA,es_PY,es_PE,es_PR,es_ES,es_UY,es_VE,sv_SE,tl_PH,tg_TJ,th_TH,uk_UA,ur_PK,vi_VN,cy_GB'),
    ReceivesInfoEmails boolean OPTIONS (NAMEINSOURCE 'ReceivesInfoEmails', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ReceivesAdminInfoEmails boolean OPTIONS (NAMEINSOURCE 'ReceivesAdminInfoEmails', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    EmailEncodingKey string(40) OPTIONS (NAMEINSOURCE 'EmailEncodingKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'UTF-8,ISO-8859-1,Shift_JIS,ISO-2022-JP,EUC-JP,ks_c_5601-1987,Big5,GB2312'),
    ProfileId string(18) OPTIONS (NAMEINSOURCE 'ProfileId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserType string(40) OPTIONS (NAMEINSOURCE 'UserType', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Standard,PowerPartner,PowerCustomerSuccess,CustomerSuccess,Guest,CSPLitePortal,CSNOnly,SelfService'),
    LanguageLocaleKey string(40) OPTIONS (NAMEINSOURCE 'LanguageLocaleKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'en_US,de,es,fr,it,ja,sv,ko,zh_TW,zh_CN,pt_BR,nl_NL,da,th,fi,ru,es_MX'),
    EmployeeNumber string(20) OPTIONS (NAMEINSOURCE 'EmployeeNumber', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DelegatedApproverId string(18) OPTIONS (NAMEINSOURCE 'DelegatedApproverId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ManagerId string(18) OPTIONS (NAMEINSOURCE 'ManagerId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastLoginDate timestamp OPTIONS (NAMEINSOURCE 'LastLoginDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LastPasswordChangeDate timestamp OPTIONS (NAMEINSOURCE 'LastPasswordChangeDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OfflineTrialExpirationDate timestamp OPTIONS (NAMEINSOURCE 'OfflineTrialExpirationDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OfflinePdaTrialExpirationDate timestamp OPTIONS (NAMEINSOURCE 'OfflinePdaTrialExpirationDate', UPDATABLE FALSE, NATIVE_TYPE 'datetime', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPermissionsMarketingUser boolean OPTIONS (NAMEINSOURCE 'UserPermissionsMarketingUser', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPermissionsOfflineUser boolean OPTIONS (NAMEINSOURCE 'UserPermissionsOfflineUser', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPermissionsCallCenterAutoLogin boolean OPTIONS (NAMEINSOURCE 'UserPermissionsCallCenterAutoLogin', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPermissionsMobileUser boolean OPTIONS (NAMEINSOURCE 'UserPermissionsMobileUser', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPermissionsSFContentUser boolean OPTIONS (NAMEINSOURCE 'UserPermissionsSFContentUser', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPermissionsKnowledgeUser boolean OPTIONS (NAMEINSOURCE 'UserPermissionsKnowledgeUser', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPermissionsInteractionUser boolean OPTIONS (NAMEINSOURCE 'UserPermissionsInteractionUser', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPermissionsSupportUser boolean OPTIONS (NAMEINSOURCE 'UserPermissionsSupportUser', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ForecastEnabled boolean OPTIONS (NAMEINSOURCE 'ForecastEnabled', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    UserPreferencesActivityRemindersPopup boolean OPTIONS (NAMEINSOURCE 'UserPreferencesActivityRemindersPopup', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPreferencesEventRemindersCheckboxDefault boolean OPTIONS (NAMEINSOURCE 'UserPreferencesEventRemindersCheckboxDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPreferencesTaskRemindersCheckboxDefault boolean OPTIONS (NAMEINSOURCE 'UserPreferencesTaskRemindersCheckboxDefault', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPreferencesReminderSoundOff boolean OPTIONS (NAMEINSOURCE 'UserPreferencesReminderSoundOff', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPreferencesDisableAutoSubForFeeds boolean OPTIONS (NAMEINSOURCE 'UserPreferencesDisableAutoSubForFeeds', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPreferencesApexPagesDeveloperMode boolean OPTIONS (NAMEINSOURCE 'UserPreferencesApexPagesDeveloperMode', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPreferencesHideCSNGetChatterMobileTask boolean OPTIONS (NAMEINSOURCE 'UserPreferencesHideCSNGetChatterMobileTask', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPreferencesHideCSNDesktopTask boolean OPTIONS (NAMEINSOURCE 'UserPreferencesHideCSNDesktopTask', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    UserPreferencesOptOutOfTouch boolean OPTIONS (NAMEINSOURCE 'UserPreferencesOptOutOfTouch', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContactId string(18) OPTIONS (NAMEINSOURCE 'ContactId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AccountId string(18) OPTIONS (NAMEINSOURCE 'AccountId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CallCenterId string(18) OPTIONS (NAMEINSOURCE 'CallCenterId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Extension string(40) OPTIONS (NAMEINSOURCE 'Extension', NATIVE_TYPE 'phone', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FederationIdentifier string(512) OPTIONS (NAMEINSOURCE 'FederationIdentifier', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    AboutMe string(1000) OPTIONS (NAMEINSOURCE 'AboutMe', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CurrentStatus string(1000) OPTIONS (NAMEINSOURCE 'CurrentStatus', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    FullPhotoUrl string(1024) OPTIONS (NAMEINSOURCE 'FullPhotoUrl', UPDATABLE FALSE, NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    SmallPhotoUrl string(1024) OPTIONS (NAMEINSOURCE 'SmallPhotoUrl', UPDATABLE FALSE, NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DigestFrequency string(40) OPTIONS (NAMEINSOURCE 'DigestFrequency', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true', "teiid_sf:Picklist Values" 'D,W,N'),
    DefaultGroupNotificationFrequency string(40) OPTIONS (NAMEINSOURCE 'DefaultGroupNotificationFrequency', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true', "teiid_sf:Picklist Values" 'P,D,W,N'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Group__DelegatedApproverId FOREIGN KEY(DelegatedApproverId) REFERENCES Group_ (Id) OPTIONS (NAMEINSOURCE 'DelegatedUsers'),
    CONSTRAINT FK_Profile_ProfileId FOREIGN KEY(ProfileId) REFERENCES Profile (Id) OPTIONS (NAMEINSOURCE 'Users'),
    CONSTRAINT FK_User__DelegatedApproverId FOREIGN KEY(DelegatedApproverId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'DelegatedUsers'),
    CONSTRAINT FK_User__ManagerId FOREIGN KEY(ManagerId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'ManagedUsers'),
    CONSTRAINT FK_UserRole_UserRoleId FOREIGN KEY(UserRoleId) REFERENCES UserRole (Id) OPTIONS (NAMEINSOURCE 'Users')
) OPTIONS (NAMEINSOURCE 'User', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE UserFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_User__ParentId FOREIGN KEY(ParentId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'Feeds')
) OPTIONS (NAMEINSOURCE 'UserFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE UserLicense (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    LicenseDefinitionKey string(40) OPTIONS (NAMEINSOURCE 'LicenseDefinitionKey', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Name string(40) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MonthlyLoginsUsed integer OPTIONS (NAMEINSOURCE 'MonthlyLoginsUsed', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MonthlyLoginsEntitlement integer OPTIONS (NAMEINSOURCE 'MonthlyLoginsEntitlement', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'UserLicense', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE UserPreference (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    UserId string(18) OPTIONS (NAMEINSOURCE 'UserId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Preference string(40) OPTIONS (NAMEINSOURCE 'Preference', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" '57,58,91,92,93,94,96,97,98,99'),
    Value_ string(1333) OPTIONS (NAMEINSOURCE 'Value', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_User__UserId FOREIGN KEY(UserId) REFERENCES User_ (Id) OPTIONS (NAMEINSOURCE 'UserPreferences')
) OPTIONS (NAMEINSOURCE 'UserPreference', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE UserProfileFeed (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'TrackedChange,UserStatus,TextPost,AdvancedTextPost,LinkPost,ContentPost,PollPost,RypplePost,ProfileSkillPost,DashboardComponentSnapshot,ApprovalPost,CaseCommentPost,ReplyPost,EmailMessageEvent,CallLogPost,ChangeStatusPost,AttachArticleEvent,MilestoneEvent,ActivityEvent,ChatTranscriptPost,CollaborationGroupCreated,CollaborationGroupUnarchived,SocialPost,QuestionPost,FacebookPost,BasicTemplateFeedItem,CreateRecordEvent,CanvasPost,AnnouncementPost'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CommentCount integer OPTIONS (NAMEINSOURCE 'CommentCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LikeCount integer OPTIONS (NAMEINSOURCE 'LikeCount', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Title string(255) OPTIONS (NAMEINSOURCE 'Title', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Body string(5000) OPTIONS (NAMEINSOURCE 'Body', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    LinkUrl string(1000) OPTIONS (NAMEINSOURCE 'LinkUrl', NATIVE_TYPE 'url', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RelatedRecordId string(18) OPTIONS (NAMEINSOURCE 'RelatedRecordId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentData blob OPTIONS (NAMEINSOURCE 'ContentData', NATIVE_TYPE 'base64', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentFileName string(255) OPTIONS (NAMEINSOURCE 'ContentFileName', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentDescription string(1000) OPTIONS (NAMEINSOURCE 'ContentDescription', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentType string(120) OPTIONS (NAMEINSOURCE 'ContentType', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ContentSize integer OPTIONS (NAMEINSOURCE 'ContentSize', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    InsertedById string(18) OPTIONS (NAMEINSOURCE 'InsertedById', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'UserProfileFeed', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'false', "teiid_sf:Supports Delete" 'false', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'false', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE UserRole (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ParentRoleId string(18) OPTIONS (NAMEINSOURCE 'ParentRoleId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    RollupDescription string(80) OPTIONS (NAMEINSOURCE 'RollupDescription', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    OpportunityAccessForAccountOwner string(40) OPTIONS (NAMEINSOURCE 'OpportunityAccessForAccountOwner', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    CaseAccessForAccountOwner string(40) OPTIONS (NAMEINSOURCE 'CaseAccessForAccountOwner', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    ContactAccessForAccountOwner string(40) OPTIONS (NAMEINSOURCE 'ContactAccessForAccountOwner', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,Read,Edit'),
    ForecastUserId string(18) OPTIONS (NAMEINSOURCE 'ForecastUserId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MayForecastManagerShare boolean OPTIONS (NAMEINSOURCE 'MayForecastManagerShare', UPDATABLE FALSE, NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    PortalAccountId string(18) OPTIONS (NAMEINSOURCE 'PortalAccountId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    PortalType string(40) OPTIONS (NAMEINSOURCE 'PortalType', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'None,CustomerPortal,Partner'),
    PortalAccountOwnerId string(18) OPTIONS (NAMEINSOURCE 'PortalAccountOwnerId', UPDATABLE FALSE, NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'UserRole', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE Vote (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean OPTIONS (NAMEINSOURCE 'IsDeleted', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ParentId string(18) OPTIONS (NAMEINSOURCE 'ParentId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Type string(40) OPTIONS (NAMEINSOURCE 'Type', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Up,Down,1,2,3,4,5'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Idea_ParentId FOREIGN KEY(ParentId) REFERENCES Idea (Id) OPTIONS (NAMEINSOURCE 'Votes'),
    CONSTRAINT FK_IdeaComment_ParentId FOREIGN KEY(ParentId) REFERENCES IdeaComment (Id) OPTIONS (NAMEINSOURCE 'Votes'),
    CONSTRAINT FK_Solution_ParentId FOREIGN KEY(ParentId) REFERENCES Solution (Id) OPTIONS (NAMEINSOURCE 'Votes')
) OPTIONS (NAMEINSOURCE 'Vote', "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE WebLink (
    Id string(18) NOT NULL AUTO_INCREMENT OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    PageOrSobjectType string(40) OPTIONS (NAMEINSOURCE 'PageOrSobjectType', UPDATABLE FALSE, NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'Account,Activity,Asset,Campaign,CampaignMember,Case,Contact,ContentVersion,Contract,CustomPageItem,DashboardComponent,Event,Idea,Lead,Opportunity,OpportunityLineItem,Product2,Solution,Task,User'),
    Name string(240) OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    IsProtected boolean OPTIONS (NAMEINSOURCE 'IsProtected', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Url string(1048576) OPTIONS (NAMEINSOURCE 'Url', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    EncodingKey string(40) OPTIONS (NAMEINSOURCE 'EncodingKey', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'UTF-8,ISO-8859-1,Shift_JIS,ISO-2022-JP,EUC-JP,ks_c_5601-1987,Big5,GB2312'),
    LinkType string(40) OPTIONS (NAMEINSOURCE 'LinkType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'url,sControl,javascript,page,flow'),
    OpenType string(40) OPTIONS (NAMEINSOURCE 'OpenType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'newWindow,sidebar,noSidebar,replace,onClickJavaScript'),
    Height integer OPTIONS (NAMEINSOURCE 'Height', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Width integer OPTIONS (NAMEINSOURCE 'Width', NATIVE_TYPE 'int', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    ShowsLocation boolean OPTIONS (NAMEINSOURCE 'ShowsLocation', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    HasScrollbars boolean OPTIONS (NAMEINSOURCE 'HasScrollbars', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    HasToolbar boolean OPTIONS (NAMEINSOURCE 'HasToolbar', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    HasMenubar boolean OPTIONS (NAMEINSOURCE 'HasMenubar', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    ShowsStatus boolean OPTIONS (NAMEINSOURCE 'ShowsStatus', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsResizable boolean OPTIONS (NAMEINSOURCE 'IsResizable', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Position string(40) OPTIONS (NAMEINSOURCE 'Position', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'fullScreen,none,topLeft'),
    ScontrolId string(18) OPTIONS (NAMEINSOURCE 'ScontrolId', NATIVE_TYPE 'reference', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    MasterLabel string(240) OPTIONS (NAMEINSOURCE 'MasterLabel', NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    Description string(1000) OPTIONS (NAMEINSOURCE 'Description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'textarea', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    DisplayType string(40) OPTIONS (NAMEINSOURCE 'DisplayType', NATIVE_TYPE 'restrictedpicklist', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false', "teiid_sf:Picklist Values" 'L,B,M'),
    RequireRowSelection boolean OPTIONS (NAMEINSOURCE 'RequireRowSelection', NATIVE_TYPE 'boolean', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    NamespacePrefix string(15) OPTIONS (NAMEINSOURCE 'NamespacePrefix', UPDATABLE FALSE, NATIVE_TYPE 'string', "teiid_sf:calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'WebLink', UPDATABLE TRUE, "teiid_sf:Custom" 'false', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');

CREATE FOREIGN TABLE Media_Prep_Order_Recipe_Step__c (
    Id string(18) NOT NULL AUTO_INCREMENT DEFAULT 'sf default' OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:Calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    OwnerId string(18) NOT NULL DEFAULT 'sf default' OPTIONS (NAMEINSOURCE 'OwnerId', NATIVE_TYPE 'reference', "teiid_sf:Calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean NOT NULL DEFAULT 'sf default' OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE '_boolean', "teiid_sf:Calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) DEFAULT 'sf default' OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:Calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    CONSTRAINT Id_PK PRIMARY KEY(Id)
) OPTIONS (NAMEINSOURCE 'Media_Prep_Order_Recipe_Step__c', UPDATABLE TRUE, CARDINALITY 1, "teiid_sf:Custom" 'true', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'true');

CREATE FOREIGN TABLE Recipe_Step_Detail__c (
    Id string(18) NOT NULL AUTO_INCREMENT DEFAULT 'sf default' OPTIONS (NAMEINSOURCE 'Id', UPDATABLE FALSE, NATIVE_TYPE 'id', "teiid_sf:Calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    IsDeleted boolean NOT NULL DEFAULT 'sf default' OPTIONS (NAMEINSOURCE 'IsDeleted', UPDATABLE FALSE, NATIVE_TYPE '_boolean', "teiid_sf:Calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Name string(80) DEFAULT 'sf default' OPTIONS (NAMEINSOURCE 'Name', NATIVE_TYPE 'string', "teiid_sf:Calculated" 'false', "teiid_sf:Custom" 'false', "teiid_sf:Defaulted on Create" 'true'),
    Order_Recipe_Steps__c string(18) NOT NULL OPTIONS (NAMEINSOURCE 'Order_Recipe_Steps__c', NATIVE_TYPE 'reference', "teiid_sf:Calculated" 'false', "teiid_sf:Custom" 'true', "teiid_sf:Defaulted on Create" 'false'),
    CONSTRAINT Id_PK PRIMARY KEY(Id),
    CONSTRAINT FK_Media_Prep_Order_Recipe_Step__c_Order_Recipe_Steps__c FOREIGN KEY(Order_Recipe_Steps__c) REFERENCES Media_Prep_Order_Recipe_Step__c (Id) OPTIONS (NAMEINSOURCE 'Recipe_Step_Details__r')
) OPTIONS (NAMEINSOURCE 'Recipe_Step_Detail__c', UPDATABLE TRUE, CARDINALITY 1, "teiid_sf:Custom" 'true', "teiid_sf:Supports Create" 'true', "teiid_sf:Supports Delete" 'true', "teiid_sf:Supports Merge" 'false', "teiid_sf:Supports Query" 'true', "teiid_sf:Supports Replicate" 'true', "teiid_sf:Supports Retrieve" 'true', "teiid_sf:Supports Search" 'false');
