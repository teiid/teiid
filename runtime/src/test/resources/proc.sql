CREATE virtual procedure point_inside_store (
                                pos_x float
                               ,pos_y float
          ) RETURNS (
                 "insideFence" integer
          ) AS
          BEGIN
             DECLARE integer insideFence = 0 ;
             DECLARE float lowerLimit = 0.0 ;
             DECLARE float upperLimit = 17.0 ;
             DECLARE float leftLimit = 0.0 ;
             DECLARE float rightLimit = 53.0 ;
             IF (
                  pos_x >= leftLimit
                  AND pos_x <= rightLimit
                  AND pos_y >= lowerLimit
                  AND pos_y <= upperLimit
             )
             BEGIN
                  insideFence = 1 ;
             END
             SELECT
                    insideFence ;
         END;