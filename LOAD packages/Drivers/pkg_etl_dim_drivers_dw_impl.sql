--==============================================================
-- PACKAGE dw_level.pkg_etl_dim_drivers_dw:                 BODY
--==============================================================

CREATE OR REPLACE PACKAGE BODY dw_level.pkg_etl_dim_drivers_dw
-- Package Load data from storage level to DW level
--
AS

--Procedure to load data of t_sa_drivers from SA_* level to DW_CL_* level
    PROCEDURE load_drivers
    AS
    BEGIN
        
        DECLARE
        
        TYPE curs_type IS REF CURSOR;
                       
        TYPE d_id_arr       IS TABLE OF dw_level.t_dw_cl_drivers.driver_id%TYPE;
        TYPE d_desc_arr     IS TABLE OF dw_level.t_dw_cl_drivers.driver_desc%TYPE;
        TYPE lcns_t_arr     IS TABLE OF dw_level.t_dw_cl_drivers.license_type%TYPE;
        TYPE d_fnm_arr      IS TABLE OF dw_level.t_dw_cl_drivers.driver_firstname%TYPE;
        TYPE d_lnm_arr      IS TABLE OF dw_level.t_dw_cl_drivers.driver_lastname%TYPE;
        TYPE hire_dt_arr    IS TABLE OF dw_level.t_dw_cl_drivers.hire_dt%TYPE;
        TYPE ins_dt_arr     IS TABLE OF dw_level.t_dw_cl_drivers.insert_dt%TYPE;
        TYPE upd_dt_arr     IS TABLE OF dw_level.t_dw_cl_drivers.update_dt%TYPE;
        TYPE val_from_arr   IS TABLE OF dw_level.t_dw_cl_drivers.valid_from%TYPE;        
        TYPE val_to_arr     IS TABLE OF dw_level.t_dw_cl_drivers.valid_to%TYPE;
        
        var_curs        curs_type;
        var_id          d_id_arr;
        var_desc        d_desc_arr;
        var_lcns        lcns_t_arr;
        var_fnm         d_fnm_arr;
        var_lnm         d_lnm_arr;
        var_hire        hire_dt_arr;
        var_ins         ins_dt_arr;
        var_upd         upd_dt_arr;
        var_val_fr      val_from_arr;
        var_val_to      val_to_arr;
        
        BEGIN        
            OPEN var_curs FOR
                 SELECT driver_id          
                       ,driver_desc     
                       ,license_type    
                       ,driver_firstname       
                       ,driver_lastname     
                       ,hire_dt 
                       ,insert_dt  
                       ,update_dt 
                       ,valid_from
                       ,valid_to 
                 FROM sa_drivers.t_sa_drivers;
                 
            LOOP                 
                FETCH var_curs
                BULK COLLECT INTO var_id
                                 ,var_desc
                                 ,var_lcns
                                 ,var_fnm
                                 ,var_lnm
                                 ,var_hire
                                 ,var_ins
                                 ,var_upd
                                 ,var_val_fr
                                 ,var_val_to;  
                                 
                FORALL i IN 1..var_id.COUNT
                    INSERT INTO dw_level.t_dw_cl_drivers
                        VALUES ( var_id(i)
                                ,var_desc(i)
                                ,var_lcns(i)
                                ,var_fnm(i)
                                ,var_lnm(i)
                                ,var_hire(i)
                                ,var_ins(i)
                                ,var_upd(i)
                                ,var_val_fr(i)
                                ,var_val_to(i) );                
                EXIT WHEN var_curs%NOTFOUND;  
            END LOOP;
            COMMIT;
            CLOSE var_curs;
        END;
            
    END;

END;
/

execute pkg_etl_dim_drivers_dw.load_drivers;
select * from dw_level.t_dw_cl_drivers;