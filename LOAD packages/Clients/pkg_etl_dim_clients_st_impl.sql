--==============================================================
-- PACKAGE st_layer.pkg_etl_dim_clients_st:                 BODY
--==============================================================

CREATE OR REPLACE PACKAGE BODY st_layer.pkg_etl_dim_clients_st
-- Package Load data from storage level to ST layer
--
AS

--Procedure to load data of sa_t_dim_clients from SA_* level to ST_* layer
        PROCEDURE load_clients
        AS
        BEGIN
            MERGE INTO st_layer.t_st_clients  tgt
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

execute pkg_etl_dim_clients_st.load_clients;
select * from st_layer.t_st_clients;