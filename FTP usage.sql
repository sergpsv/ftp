--select * from v_tlog 
-- Send a binary file to a remote FTP server.
DECLARE
  l_conn  UTL_TCP.connection;
  l_clb   clob;
  l_file  varchar2(30);
BEGIN
--  select c_lob, filename||nvl(ext_c,'.csv') into l_clb, l_file from emailbody_att where mail_num=11;
  l_clb := to_clob('УРАААА'||chr(13)||chr(10)||'English text');
--  l_clb := get_clob('select * from tab4check  where 1=1');
  l_conn := ftp.login('10.61.40.70', '21', 'ftpuser', 'ftp_upload'); --
  ftp.put_remote_ascii_data(l_conn, 'tab4check.txt', l_clb);
  ftp.logout(l_conn);
  utl_tcp.close_all_connections;
END;
/

Необходимо организовать доступ по FTP до АПК реклама (сеть 10.78.74.128/28), выкладывать на два сервера 10.78.74.132 и .133 Port 21 списки отказников (в виде обычных текстовых файлов, 2 списка полный и инкрементальный) под пользователем bill/bill на регулярной основе.

begin
  utl_tcp.close_all_connections;
end;

-- Send a binary file to a remote FTP server.
DECLARE
  l_conn  UTL_TCP.connection;
  l_blb   blob;
  lcb     clob;
-- ftp://dealer_upload:bKB8pTPc@10.23.10.234/in
--  constr    varchar2(100) := 'ftp://bsuser2:w2011_11_11@10.23.10.144';
--  constr    varchar2(100) := 'ftp://nw:nwpass@10.78.22.6 ';
--  constr    varchar2(100) := 'ftp://KV_Sverka:SeKA4w6wVwDN@10.163.227.6';
--  constr    varchar2(100) := 'ftp://KV_Sverka:SeKA4w6wVwDN@hqpv-fs-01.megafon-retail.ru';
--  constr    varchar2(100) := 'mr_check_options:T81d6lKqEzZwnDX0GDqJ@10.61.10.130';
  constr    varchar2(100) := 'ftp://DebitorAccounting-RW:PdM52NU8YrS6PReP@10.23.10.21';
BEGIN
  select dc into lcb from t10 where ITEMNAME like '%more_than_90';
  l_blb := pck_zip.clob_compress(lcb, 'more_than_180.csv');
  l_conn := ftp.login(constr); --
--  dbms_output.put_line(ftp.get_file_mdmt(l_conn,'KF_may.csv'));
  ftp.put_remote_binary_data(l_conn, '201412\more_than_90.zip', l_blb);
  ftp.logout(l_conn);
  utl_tcp.close_all_connections;
END;
/

   
-- select * from emailbody_att where mail_num=10   


Сергей привет!

Можно попробовать ресурс тем же  имеем и паролем
Но ip 10.23.10.234,  Если у вас все получится, я тогда дам знать нашим ребятам (кто ответственный за старый ftp) и они его закроют 



ftp://hqpv-fs-01.megafon-retail.ru/
hqpv-fs-01.megafon-retail.ru
10.163.227.6

Логин/пасс
KV_Sverka
SeKA4w6wVwDN

