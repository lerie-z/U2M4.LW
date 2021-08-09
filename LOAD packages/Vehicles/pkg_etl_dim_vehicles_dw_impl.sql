--==============================================================
-- PACKAGE dw_level.pkg_etl_dim_vehicles_dw:                BODY
--==============================================================

CREATE OR REPLACE PACKAGE BODY dw_level.pkg_etl_dim_vehicles_dw
-- Package Load data from storage level to DW level
--
AS

--Procedure to load data of t_sa_vehicles from SA_* level to DW_CL_* level
    PROCEDURE load_vehicles
    AS
    BEGIN
        
        DECLARE
        CURSOR curs IS SELECT  vehicle_id
                              ,vehicle_code
                              ,vehicle_desc
                              ,vehicle_vin
                              ,vehicle_mark
                              ,insert_dt
                              ,update_dt
                              ,valid_from
                              ,valid_to
                              ,fuel_type
                       FROM sa_vehicles.t_sa_vehicles;
                       
        TYPE v_id_arr       IS TABLE OF dw_level.t_dw_cl_vehicles.vehicle_id%TYPE;
        TYPE v_code_arr     IS TABLE OF dw_level.t_dw_cl_vehicles.vehicle_code%TYPE;
        TYPE v_desc_arr     IS TABLE OF dw_level.t_dw_cl_vehicles.vehicle_desc%TYPE;
        TYPE v_vin_arr      IS TABLE OF dw_level.t_dw_cl_vehicles.vehicle_vin%TYPE;
        TYPE v_mark_arr     IS TABLE OF dw_level.t_dw_cl_vehicles.vehicle_mark%TYPE;
        TYPE ins_dt_arr     IS TABLE OF dw_level.t_dw_cl_vehicles.insert_dt%TYPE;
        TYPE upd_dt_arr     IS TABLE OF dw_level.t_dw_cl_vehicles.update_dt%TYPE;
        TYPE val_from_arr   IS TABLE OF dw_level.t_dw_cl_vehicles.valid_from%TYPE;        
        TYPE val_to_arr     IS TABLE OF dw_level.t_dw_cl_vehicles.valid_to%TYPE;
        TYPE fuel_type_arr  IS TABLE OF dw_level.t_dw_cl_vehicles.fuel_type%TYPE;
        
        var_id          v_id_arr;
        var_code        v_code_arr;
        var_desc        v_desc_arr;
        var_vin         v_vin_arr;
        var_mark        v_mark_arr;
        var_ins         ins_dt_arr;
        var_upd         upd_dt_arr;
        var_val_from    val_from_arr;
        var_val_to      val_to_arr;
        var_fuel        fuel_type_arr;
        
        BEGIN        
            OPEN curs;
            LOOP
                FETCH curs BULK COLLECT 
                    INTO var_id
                        ,var_code
                        ,var_desc
                        ,var_vin
                        ,var_mark
                        ,var_ins
                        ,var_upd
                        ,var_val_from
                        ,var_val_to
                        ,var_fuel;
                                            
                FORALL i IN 1..var_id.COUNT
                    INSERT INTO dw_level.t_dw_cl_vehicles
                    VALUES (var_id(i)
                           ,var_code(i)
                           ,var_desc(i)
                           ,var_vin(i)
                           ,var_mark(i)
                           ,var_ins(i)
                           ,var_upd(i)
                           ,var_val_from(i)
                           ,var_val_to(i)
                           ,var_fuel(i) );                
                EXIT WHEN curs%NOTFOUND;  
            END LOOP;
            COMMIT;
            CLOSE curs;
        END;
            
    END;

END;
/

execute pkg_etl_dim_vehicles_dw.load_vehicles;
select * from dw_level.t_dw_cl_vehicles;