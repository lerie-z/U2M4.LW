--==============================================================
-- PACKAGE dw_cls_lvl.pkg_etl_dim_clients_dw:               BODY
--==============================================================

CREATE OR REPLACE PACKAGE BODY dw_level.pkg_etl_dim_clients_dw
-- Package Load data from storage level to DW level
--
AS

--Procedure to load data of sa_t_dim_clients from SA_* level to DW_CL_* level
        PROCEDURE load_clients
        AS
        BEGIN
            MERGE INTO dw_level.t_dw_cl_clients tgt
                USING (SELECT * FROM sa_clients.t_sa_clients) src
                    ON (src.client_id = tgt.client_id)
            WHEN NOT MATCHED 
                THEN
                    INSERT ( client_id
                            ,client_desc
                            ,client_bitrhdate
                            ,client_firstname
                            ,client_lastname
                            ,registration_dt
                            ,insert_dt
                            ,update_dt
                            ,valid_from
                            ,valid_to)
                    VALUES ( src.client_id
                            ,src.client_desc
                            ,src.client_bitrhdate
                            ,src.client_firstname
                            ,src.client_lastname
                            ,src.registration_dt
                            ,src.insert_dt
                            ,(SELECT current_date FROM dual)
                            ,(SELECT current_date FROM dual)
                            ,(SELECT add_months(TO_DATE('2010-01-01', 'yyyy-mm-dd'), 12 )-1
                                    FROM dual) )
            WHEN MATCHED 
                THEN
                    UPDATE SET tgt.client_desc      = src.client_desc
                              ,tgt.client_bitrhdate = src.client_bitrhdate
                              ,tgt.client_firstname = src.client_firstname
                              ,tgt.client_lastname  = src.client_lastname
                              ,tgt.registration_dt  = src.registration_dt
                              ,tgt.insert_dt        = src.insert_dt
                              ,tgt.update_dt        = (SELECT current_date FROM dual)
                              ,tgt.valid_from       = (SELECT current_date FROM dual)
                              ,tgt.valid_to         = (SELECT add_months(TO_DATE('2010-01-01', 'yyyy-mm-dd'), 12 )-1
                                                            FROM dual)                    
;
            COMMIT;
        END load_clients;
END;
/