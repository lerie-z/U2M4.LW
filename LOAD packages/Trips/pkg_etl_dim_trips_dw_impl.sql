--==============================================================
-- PACKAGE dw_level.pkg_etl_dim_trips_dw:                 BODY
--==============================================================

CREATE OR REPLACE PACKAGE BODY dw_level.pkg_etl_dim_trips_dw
-- Package Load data from storage level to DW level
--
AS

--Procedure to load data of t_sa_trips from SA_* level to DW_CL_* level
    PROCEDURE load_trips
    AS
    BEGIN
        
        DECLARE
        CURSOR curs IS SELECT  trip_id
                              ,trip_desc
                              ,trip_from
                              ,trip_to
                              ,trip_duration
                              ,trip_cost
                              ,payment_type
                              ,trip_date
                              ,trip_client_id
                              ,trip_driver_id
                       FROM sa_trips.t_sa_trips;
                       
        crs_trip_id           sa_trips.t_sa_trips.trip_id%TYPE;
        crs_trip_desc         sa_trips.t_sa_trips.trip_desc%TYPE;
        crs_trip_from         sa_trips.t_sa_trips.trip_from%TYPE;
        crs_trip_to           sa_trips.t_sa_trips.trip_to%TYPE;
        crs_trip_duration     sa_trips.t_sa_trips.trip_duration%TYPE;
        crs_trip_cost         sa_trips.t_sa_trips.trip_cost%TYPE;
        crs_payment_type      sa_trips.t_sa_trips.payment_type%TYPE;
        crs_trip_date         sa_trips.t_sa_trips.trip_date%TYPE;
        crs_trip_client_id    sa_trips.t_sa_trips.trip_client_id%TYPE;
        crs_trip_driver_id    sa_trips.t_sa_trips.trip_driver_id%TYPE;
        
        BEGIN        
            OPEN curs;
            LOOP
                FETCH curs INTO  crs_trip_id
                                ,crs_trip_desc
                                ,crs_trip_from
                                ,crs_trip_to
                                ,crs_trip_duration
                                ,crs_trip_cost
                                ,crs_payment_type
                                ,crs_trip_date
                                ,crs_trip_client_id
                                ,crs_trip_driver_id;
                EXIT WHEN curs%NOTFOUND;                
                INSERT INTO dw_level.t_dw_cl_trips
                VALUES (crs_trip_id
                       ,crs_trip_desc
                       ,crs_trip_from
                       ,crs_trip_to
                       ,crs_trip_duration
                       ,crs_trip_cost
                       ,crs_payment_type
                       ,crs_trip_date
                       ,crs_trip_client_id
                       ,crs_trip_driver_id);                                
            END LOOP;
            COMMIT;
            CLOSE curs;
        END;
            
    END;

END;
/