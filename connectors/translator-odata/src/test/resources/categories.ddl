CREATE FOREIGN TABLE Category
(
    CategoryID integer NOT NULL,
    Name varchar(25) NOT NULL,
    ParentCategoryID integer NOT NULL, 

    CONSTRAINT PK_CATEGORY PRIMARY KEY (CategoryID),
    CONSTRAINT FK_CATEGORY_ID FOREIGN KEY (ParentCategoryID) REFERENCES Category(CategoryID)
)