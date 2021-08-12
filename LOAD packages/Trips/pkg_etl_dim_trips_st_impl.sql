--==============================================================
-- PACKAGE st_layer.pkg_etl_dim_trips_st:                   BODY
--==============================================================

CREATE OR REPLACE PACKAGE BODY st_layer.pkg_etl_dim_trips_st
-- Package Load data from storage level to ST layer
--
AS

--Procedure to load data of t_sa_trips from SA_* level to ST_* layer
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
                       
        src_trip_id           sa_trips.t_sa_trips.trip_id%TYPE;
        src_trip_desc         sa_trips.t_sa_trips.trip_desc%TYPE;
        src_trip_from         sa_trips.t_sa_trips.trip_from%TYPE;
        src_trip_to           sa_trips.t_sa_trips.trip_to%TYPE;
        src_trip_duration     sa_trips.t_sa_trips.trip_duration%TYPE;
        src_trip_cost         sa_trips.t_sa_trips.trip_cost%TYPE;
        src_payment_type      sa_trips.t_sa_trips.payment_type%TYPE;
        src_trip_date         sa_trips.t_sa_trips.trip_date%TYPE;
        src_trip_client_id    sa_trips.t_sa_trips.trip_client_id%TYPE;
        src_trip_driver_id    sa_trips.t_sa_trips.trip_driver_id%TYPE;
        
        BEGIN        
            OPEN curs;
            LOOP
                FETCH curs INTO  src_trip_id
                                ,src_trip_desc
                                ,src_trip_from
                                ,src_trip_to
                                ,src_trip_duration
                                ,src_trip_cost
                                ,src_payment_type
                                ,src_trip_date
                                ,src_trip_client_id
                                ,src_trip_driver_id;
                EXIT WHEN curs%NOTFOUND;                
                INSERT INTO st_layer.t_st_trips
                VALUES (src_trip_id
                       ,src_trip_desc
                       ,src_trip_from
                       ,src_trip_to
                       ,src_trip_duration
                       ,src_trip_cost
                       ,src_payment_type
                       ,src_trip_date
                       ,src_trip_client_id
                       ,src_trip_driver_id);                                
            END LOOP;
            COMMIT;
            CLOSE curs;
        END;
            
    END;

END;
/

execute pkg_etl_dim_trips_st.load_trips;
select * from st_layer.t_st_trips;