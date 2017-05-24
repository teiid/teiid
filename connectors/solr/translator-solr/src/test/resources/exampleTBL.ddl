CREATE FOREIGN TABLE example (
  price float,
  weight float,
  popularity integer, 
  name string PRIMARy KEY, 
  purchasets timestamp,
  startDate timestamp,
  purchasetime time,
  purchasedate date,
  field string OPTIONS (NAMEINSOURCE 'nis') 
);

