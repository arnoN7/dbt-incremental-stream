select * from PERSO.ARO_STG.ADD_CLIENTS limit 10;
insert into PERSO.ARO_STG.ADD_CLIENTS values (1, 'Daniel', 'Hanna', '1986-02-10', current_timestamp());
CREATE STAGE PERSO.ARO_STG.CLIENTS;