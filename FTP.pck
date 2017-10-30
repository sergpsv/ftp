CREATE OR REPLACE PACKAGE FTP
authid current_user
 is
-- --------------------------------------------------------------------------
-- Name         : http://www.oracle-base.com/dba/miscellaneous/ftp.pks
-- Author       : sparshukov
-- original avtor : DR Timothy S Hall
-- Description  : Basic FTP API.
-- Requirements : UTL_TCP
-- Ammedments   :
--   When         Who       What
/*   ===========  ========  =================================================
--   14-AUG-2003  Tim Hall  Initial Creation
--   10-MAR-2004  Tim Hall  Add convert_crlf procedure.
--                          Make get_passive function visible.
--                          Added get_direct and put_direct procedures.
    2012/10/13   обработка исключений в put_remote_(binary\ascii)_data
                 обработка исключений в get_passive
    2013/04/10   l_file:= replace(p_file ,'/','\');
                 l_file:= substr(l_file, instr(l_file,'\',-1)+1);

-- --------------------------------------------------------------------------
*/

 gpv_storemethod        varchar2(4)  := 'STOR'; --- безусловная перезапись
                                          --  STOU - если файл существует - будет ошибка
 gpv_debug              boolean := FALSE;
 gpv_remoteFScodepage   varchar2(100):='';
 gpv_featlist           TStringList:=TStringList();


TYPE t_string_table IS TABLE OF VARCHAR2(32767);



FUNCTION get_passive (p_conn  IN OUT NOCOPY  UTL_TCP.connection) RETURN UTL_TCP.connection;

PROCEDURE logout (p_conn   IN OUT NOCOPY  UTL_TCP.connection,
                  p_reply  IN             BOOLEAN := TRUE);

PROCEDURE send_command (p_conn     IN OUT NOCOPY  UTL_TCP.connection,
                        p_command  IN             VARCHAR2,
                        p_reply    IN             BOOLEAN := TRUE,
                        p_sleeptime in number default 0);

FUNCTION get_local_ascii_data (p_dir   IN  VARCHAR2,
                               p_file  IN  VARCHAR2)                        RETURN CLOB;

FUNCTION get_local_binary_data (p_dir   IN  VARCHAR2,
                                p_file  IN  VARCHAR2)                       RETURN BLOB;

FUNCTION get_remote_ascii_data (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                                p_file  IN             VARCHAR2)            RETURN CLOB;

FUNCTION get_remote_binary_data (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                                 p_file  IN             VARCHAR2)           RETURN BLOB;

PROCEDURE put_local_ascii_data (p_data  IN  CLOB,
                                p_dir   IN  VARCHAR2,
                                p_file  IN  VARCHAR2);

PROCEDURE put_local_binary_data (p_data  IN  BLOB,
                                 p_dir   IN  VARCHAR2,
                                 p_file  IN  VARCHAR2);

PROCEDURE put_remote_ascii_data (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                                 p_file  IN             VARCHAR2,
                                 p_data  IN             CLOB,
                                 p_codepage in varchar2 default 'CL8MSWIN1251');

PROCEDURE put_remote_binary_data (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                                  p_file  IN             VARCHAR2,
                                  p_data  IN             BLOB);


PROCEDURE help (p_conn  IN OUT NOCOPY  UTL_TCP.connection);
PROCEDURE ascii (p_conn  IN OUT NOCOPY  UTL_TCP.connection);
PROCEDURE binary (p_conn  IN OUT NOCOPY  UTL_TCP.connection);
PROCEDURE convert_crlf (p_status  IN  BOOLEAN);

function GetSystem(p_conn  IN OUT NOCOPY  UTL_TCP.connection) return varchar2;
  procedure print_connection(p_str varchar2, p_conn in out nocopy utl_tcp.connection);
procedure GetFeatList(p_conn  IN OUT NOCOPY  UTL_TCP.connection);
  function CreatePath(p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                     p_path in varchar2) return number;
  PROCEDURE get_reply (p_conn  IN OUT NOCOPY  UTL_TCP.connection);
  FUNCTION get_remote_file_list (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                               p_mask  IN             VARCHAR2 default null)
RETURN TStringList;
  FUNCTION get_file_MDMT (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                        p_file  IN             VARCHAR2 default null)
RETURN date;
  FUNCTION login (p_ftpadr in varchar2,  p_supress in number default 0) RETURN UTL_TCP.connection;
  FUNCTION login (p_host  IN  VARCHAR2,
                p_port  IN  VARCHAR2,
                p_user  IN  VARCHAR2,
                p_pass  IN  VARCHAR2,
                p_path  in  varchar2 default null,
                p_supress in number default 0) RETURN UTL_TCP.connection;
  procedure TestOrCreateDir(p_conn  IN OUT NOCOPY  UTL_TCP.connection, dir varchar2, p_moveOrTest number);
end;
/
CREATE OR REPLACE PACKAGE BODY FTP is
-- --------------------------------------------------------------------------
-- Name         : http://www.oracle-base.com/dba/miscellaneous/ftp.pkb
-- Author       : DR Timothy S Hall
-- Description  : Basic FTP API.
-- Requirements : http://www.oracle-base.com/dba/miscellaneous/ftp.pks
-- Ammedments   :
--   When         Who       What
--   ===========  ========  =================================================
--   14-AUG-2003  Tim Hall  Initial Creation
--   10-MAR-2004  Tim Hall  Add convert_crlf procedure.
--                          Incorporate CRLF conversion functionality into
--                          put_local_ascii_data and put_remote_ascii_data
--                          functions.
--                          Make get_passive function visible.
--                          Added get_direct and put_direct procedures.
--   23-DEC-2004  Tim Hall  The get_reply procedure was altered to deal with
--                          banners starting with 4 white spaces. This fix is
--                          a small variation on the resolution provided by
--                          Gary Mason who spotted the bug.
--   10-NOV-2005  Tim Hall  Addition of get_reply after doing a transfer to
--                          pickup the 226 Transfer complete message. This
--                          allows gets and puts with a single connection.
--                          Issue spotted by Trevor Woolnough.
-- --------------------------------------------------------------------------


g_reply         t_string_table := t_string_table();
g_binary        BOOLEAN := TRUE;
g_debug         BOOLEAN := TRUE;
g_convert_crlf  BOOLEAN := TRUE;

l_sessionID     number  :=0;
l_ftpsystem     varchar2(100);
gl_dbchrset     varchar2(100);

ex_ascii      EXCEPTION;


--PROCEDURE get_reply (p_conn  IN OUT NOCOPY  UTL_TCP.connection);

PROCEDURE debug (p_text  IN  VARCHAR2);


------------------------------------------------------------------------
function cs_fromftp(p_str varchar2) return varchar2
is
begin
  if length(gpv_remoteFScodepage)=0 then 
    return convert(p_str, gl_dbchrset, gpv_remoteFScodepage);
  else
    return convert(p_str, gl_dbchrset, 'CL8MSWIN1251');
  end if;
end;

------------------------------------------------------------------------
function cs_toftp(p_str varchar2) return varchar2
is
begin
  if length(gpv_remoteFScodepage)=0 then 
    return convert(p_str, gpv_remoteFScodepage);
  else
    return convert(p_str, 'CL8MSWIN1251');
  end if;
end;


------------------------------------------------------------------------
function what_part return varchar2 is 
begin
  if length(sys_context('jm_ctx','procdr'))>0 then 
    return sys_context('jm_ctx','procdr');
  else
    return 'ftp';
  end if;
end;

  ------------------------------------------------------------------------
  -- получение одного хоста
function GetOneRecip(p_recip in varchar2, 
            p_fHost in out varchar2, p_fLogin in out varchar2, 
            p_fPass in out varchar2, p_fPath in out varchar2) return number
is
    l_str varchar2(500) := replace(p_recip,'\','/');
begin
    if upper(p_recip) like 'FTP://%' then 
      l_str := substr(l_str, 7);
    else
      l_str := l_str;
    end if;
    -- 'FTP://dealer_upload:bKB8pTPc@10.23.10.234'
    p_fLogin := substr(l_str, 1, instr(l_str,':')-1);
    p_fPass  := substr(l_str, 1+instr(l_str,':') , instr(l_str,'@')-1-length(substr(l_str, 1, instr(l_str,':'))));
    if instr(l_str,'/') > 0 then 
        p_fhost  := substr(l_str, 1+instr(l_str,'@') , instr(l_str,'/')-1-length(substr(l_str, 1, instr(l_str,'@'))));
        p_fPath  := substr(l_str, 1+instr(l_str,'/') );
    else
        p_fhost  := substr(l_str, 1+instr(l_str,'@'));
        p_fPath  := '';
    end if;
    if length(p_fHost)>0 and length(p_fLogin)>0 and length(p_fPass)>0 then 
      return 0;
    else
      return -1;
    end if;
end;
  
-------------------------------------------------------------------------
procedure GetFeatList(p_conn  IN OUT NOCOPY  UTL_TCP.connection)
IS
  l_featlist TStringList := TStringList();
  l_lastTail number;
BEGIN
  l_lastTail := nvl(g_reply.last, 1)+1;
  send_command(p_conn, 'FEAT', TRUE, 100);
  --dbms_output.put_line('l_lastTail= '||l_lastTail||' g_reply.last='||g_reply.last||' g_reply.count'||g_reply.count);
  for i in l_lastTail .. g_reply.count
  loop
    --dbms_output.put_line('debug:'||g_reply(i));
    if g_reply(i) not like '2%' then 
      l_featlist.extend;
      l_featlist(l_featlist.count) := trim(g_reply(i));
      if l_featlist(l_featlist.count) = 'UTF8' then 
        gpv_remoteFScodepage := 'AL32UTF8';
      end if;
    end if;
  end loop;
  gpv_featlist := l_featlist;
  --dbms_output.put_line('GetFeatList : l_featlist.count='||l_featlist.count);
exception
  when others then 
    log_ovart(-1,'ftp', dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
END;       
  
-------------------------------------------------------------------------
FUNCTION login (p_ftpadr in varchar2,  p_supress in number default 0) RETURN UTL_TCP.connection 
IS
  l_conn    UTL_TCP.connection;
  p_fHost   varchar2(200);
  p_fLogin  varchar2(200);
  p_fPass   varchar2(200);
  p_fPath   varchar2(200);
BEGIN
  if GetOneRecip(p_ftpadr, p_fHost, p_fLogin, p_fPass, p_fPath) = 0 then
    l_conn := login (p_fHost, '21', p_fLogin, p_fPass, p_fPath, p_supress);
    RETURN l_conn;
  else
    RETURN null;
  end if;
END;


--------------------------------------------------------------------------
-- проверка директории относительно текущей точки входа
-- p_moveOrTest = 0 только тестирование и создание при необходимости
--              = 1 тестирование, создание при необходимости и переход в нее
procedure TestOrCreateDir(p_conn  IN OUT NOCOPY  UTL_TCP.connection, dir varchar2, p_moveOrTest number)
is
begin
  begin
    ftp.send_command(p_conn,'CWD '||dir, TRUE);        
    if p_moveOrTest=0 then 
      ftp.send_command(p_conn,'CWD ..', TRUE);        
    end if;
  exception 
    when others then 
      begin
        ftp.send_command(p_conn,'MKD '||dir, TRUE);        
        if p_moveOrTest=1 then 
          ftp.send_command(p_conn,'CWD '||dir, TRUE);        
        end if;
      exception 
        when others then 
          log_ovart(-1,what_part, 'attempt to make dir ('||dir||') and change it to currect is fails ');
          log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
      end;
  end;

exception
  when others then 
    log_ovart(-1,'ftp', dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
END;       
      
--------------------------------------------------------------------------
FUNCTION login (p_host  IN  VARCHAR2,
                p_port  IN  VARCHAR2,
                p_user  IN  VARCHAR2,
                p_pass  IN  VARCHAR2,
                p_path  in  varchar2 default null,
                p_supress in number default 0) RETURN UTL_TCP.connection 
IS
  l_conn  UTL_TCP.connection;
  l_path    varchar2(250);
  dir       varchar2(100);
  l_stop    number          :=10;
  l_str     varchar2(1000);
BEGIN
  g_reply.delete;
  dbms_output.put_line('p_host='||p_host);
  dbms_output.put_line('p_port='||p_port);
  dbms_output.put_line('p_user='||p_user);
  dbms_output.put_line('p_pass='||p_pass);
  dbms_output.put_line('p_path='||p_path);
  l_conn := UTL_TCP.open_connection(p_host, p_port, tx_timeout => 60);
  get_reply (l_conn);
  send_command(l_conn, 'USER ' || p_user, TRUE);
  send_command(l_conn, 'PASS ' || p_pass, TRUE);
  l_str:=GetSystem(l_conn);
  gpv_remoteFScodepage := '';
  gpv_featlist.delete;
  GetFeatList(l_conn);
  l_path := replace(p_path,'\', '/');
  debug('p-path =  '||l_path);
  if length(l_path)>0 then 
    if l_path like '/%' then l_path := substr(l_path,2); end if;
    while length(l_path)>0 or l_stop=0
    loop
      l_stop := instr(l_path,'/');
      if l_stop=0 then 
        dir := l_path; l_path := '';
      else
        dir := substr(l_path, 1, l_stop-1);
      end if;
      begin
        ftp.send_command(l_conn,'CWD '||dir, TRUE);        
      exception 
        when others then 
          begin
            ftp.send_command(l_conn,'MKD '||dir, TRUE);        
            ftp.send_command(l_conn,'CWD '||dir, TRUE);        
          exception 
            when others then 
              log_ovart(-1,what_part, 'attempt to make dir ('||dir||') and change it to currect is fails ');
              log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
          end;
      end;
      l_path := substr(l_path, l_stop+1);
      l_stop := instr(l_path,'/');
    end loop;
  end if;
  if p_supress=0 then 
    log_ovart(-1,what_part,'login на сервер '||p_host||' пользователем '||p_user||case when length(p_path)>0 then ' в директорию('||p_path||')'else '' end);
  end if;
  print_connection('Глобальный коннект', l_conn);
    debug('l_ftpsystem='||l_ftpsystem||', l_remoteFScodepage='||gpv_remoteFScodepage||', gpv_featlist.count='||gpv_featlist.count);
    if nvl(gpv_featlist.count,0)>0 then 
      for i in gpv_featlist.first .. gpv_featlist.last
      loop
        debug(gpv_featlist(i));
      end loop;
    end if;

  execute immediate 'SELECT value FROM NLS_DATABASE_PARAMETERS where parameter=''NLS_CHARACTERSET''' 
                     into gl_dbchrset;

  RETURN l_conn;
END;

--------------------------------------------------------------------------
FUNCTION get_passive (p_conn  IN OUT NOCOPY  UTL_TCP.connection)
  RETURN UTL_TCP.connection IS
  l_conn    UTL_TCP.connection;
  l_reply   VARCHAR2(32767);
  l_host    VARCHAR(100);
  l_port1   NUMBER(10);
  l_port2   NUMBER(10);
BEGIN
  --debug('send PASV ');
  send_command(p_conn, 'PASV', TRUE);
  l_reply := g_reply(g_reply.last);
  --debug('get reply - '||l_reply);

  l_reply := REPLACE(SUBSTR(l_reply, INSTR(l_reply, '(') + 1, (INSTR(l_reply, ')')) - (INSTR(l_reply, '('))-1), ',', '.');
  l_host  := SUBSTR(l_reply, 1, INSTR(l_reply, '.', 1, 4)-1);

  l_port1 := TO_NUMBER(SUBSTR(l_reply, INSTR(l_reply, '.', 1, 4)+1, (INSTR(l_reply, '.', 1, 5)-1) - (INSTR(l_reply, '.', 1, 4))));
  l_port2 := TO_NUMBER(SUBSTR(l_reply, INSTR(l_reply, '.', 1, 5)+1));

  --debug('l_host='||l_host||' (256 * l_port1 + l_port2)='||(256 * l_port1 + l_port2)); 
  l_conn := utl_tcp.open_connection(l_host, 256 * l_port1 + l_port2);
  return l_conn;
exception
  when others then 
    log_ovart(-1, what_part, 'Ошибка при переходе в пассивный режим. Получен ответ "'||l_reply||'"');
    log_ovart(-1, what_part, 'Определены хост="'||l_host||'", порт1="'||l_port1||'", порт2="'||l_port2||'"');
    log_ovart(-1, what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
    raise;
END;
--------------------------------------------------------------------------


PROCEDURE logout(p_conn   IN OUT NOCOPY  UTL_TCP.connection,
                 p_reply  IN             BOOLEAN := TRUE) 
AS
  l_dat  date;
BEGIN
  debug('----------------- ');
  UTL_TCP.flush(p_conn);

  --log_ovart(-1,what_part,'5-ти секундная задержка');
  l_dat := sysdate + 5/86400;
  while sysdate < l_dat
  loop
    null;
  end loop;
  send_command(p_conn, 'QUIT', p_reply);
  UTL_TCP.close_connection(p_conn);
END;
-- --------------------------------------------------------------------------



-- --------------------------------------------------------------------------
PROCEDURE send_command (p_conn     IN OUT NOCOPY  UTL_TCP.connection,
                        p_command  IN             VARCHAR2,
                        p_reply    IN             BOOLEAN := TRUE,
                        p_sleeptime in number default 0) 
IS
  l_result  PLS_INTEGER;
  l_curtime number := p_sleeptime; -- 1 sec
BEGIN
  l_result := UTL_TCP.write_line(p_conn, p_command);
  dbms_output.put_line('>'||p_command);
  if p_sleeptime > 0 then 
    l_curtime := dbms_utility.get_time;
     while dbms_utility.get_time < l_curtime +  p_sleeptime
     loop
       null;
     end loop;
  end if;
  
  IF p_reply THEN
    get_reply(p_conn);
  END IF;
END;
-- --------------------------------------------------------------------------



-- --------------------------------------------------------------------------
PROCEDURE get_reply (p_conn  IN OUT NOCOPY  UTL_TCP.connection) IS
-- --------------------------------------------------------------------------
  l_reply_code  VARCHAR2(3) := NULL;
BEGIN
  LOOP
    g_reply.extend;
    g_reply(g_reply.last) := UTL_TCP.get_line(p_conn, TRUE);
    debug(g_reply(g_reply.last));
    IF l_reply_code IS NULL THEN
      l_reply_code := SUBSTR(g_reply(g_reply.last), 1, 3);
    END IF;
    IF SUBSTR(l_reply_code, 1, 1) = '5' THEN
      RAISE_APPLICATION_ERROR(-20000, g_reply(g_reply.last));
    ELSIF (SUBSTR(g_reply(g_reply.last), 1, 3) = l_reply_code AND
           SUBSTR(g_reply(g_reply.last), 4, 1) = ' ') THEN
      EXIT;
    END IF;
  END LOOP;
EXCEPTION
  WHEN UTL_TCP.END_OF_INPUT THEN
    NULL;
END;
-- --------------------------------------------------------------------------



-- --------------------------------------------------------------------------
FUNCTION get_local_ascii_data (p_dir   IN  VARCHAR2,
                               p_file  IN  VARCHAR2)
  RETURN CLOB IS
-- --------------------------------------------------------------------------
  l_bfile   BFILE;
  l_data    CLOB;
BEGIN
  DBMS_LOB.createtemporary (lob_loc => l_data,
                            cache   => TRUE,
                            dur     => DBMS_LOB.call);

  l_bfile := BFILENAME(p_dir, p_file);
  DBMS_LOB.fileopen(l_bfile, DBMS_LOB.file_readonly);
  DBMS_LOB.loadfromfile(l_data, l_bfile, DBMS_LOB.getlength(l_bfile));
  DBMS_LOB.fileclose(l_bfile);

  RETURN l_data;
END;
-- --------------------------------------------------------------------------



-- --------------------------------------------------------------------------
FUNCTION get_local_binary_data (p_dir   IN  VARCHAR2,
                                p_file  IN  VARCHAR2)
  RETURN BLOB IS
-- --------------------------------------------------------------------------
  l_bfile   BFILE;
  l_data    BLOB;
BEGIN
  DBMS_LOB.createtemporary (lob_loc => l_data,
                            cache   => TRUE,
                            dur     => DBMS_LOB.call);

  l_bfile := BFILENAME(p_dir, p_file);
  DBMS_LOB.fileopen(l_bfile, DBMS_LOB.file_readonly);
  DBMS_LOB.loadfromfile(l_data, l_bfile, DBMS_LOB.getlength(l_bfile));
  DBMS_LOB.fileclose(l_bfile);

  RETURN l_data;
END;
-- --------------------------------------------------------------------------



-- --------------------------------------------------------------------------
FUNCTION get_remote_ascii_data (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                                p_file  IN             VARCHAR2)
RETURN CLOB IS
  l_conn    UTL_TCP.connection;
  l_amount  PLS_INTEGER;
  l_buffer  VARCHAR2(32767);
  l_data    CLOB;
  l_reply   varchar2(1000);
BEGIN
  DBMS_LOB.createtemporary (lob_loc => l_data, cache => TRUE, dur => DBMS_LOB.call);
  l_conn := get_passive(p_conn);
  print_connection('локальный коннект на прием файла :', l_conn);
  send_command(p_conn, 'RETR ' || cs_toftp(p_file), TRUE);
  BEGIN
    LOOP
      l_amount := UTL_TCP.read_text (l_conn, l_buffer, 32767);
      DBMS_LOB.writeappend(l_data, l_amount, cs_fromftp(l_buffer));
    END LOOP;
  EXCEPTION
    WHEN UTL_TCP.END_OF_INPUT THEN
      NULL;
    WHEN OTHERS THEN
      log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
  END;
  UTL_TCP.close_connection(l_conn);
  get_reply(p_conn); l_reply := g_reply(g_reply.last);
  log_ovart(-1,what_part,'получен ascii файл "'||p_file||'" ('||l_reply||')');
  RETURN l_data;
END;


-- --------------------------------------------------------------------------
FUNCTION get_remote_binary_data (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                                 p_file  IN             VARCHAR2)
RETURN BLOB IS
  l_conn    UTL_TCP.connection;
  l_amount  PLS_INTEGER;
  l_buffer  RAW(32767);
  l_data    BLOB;
  l_reply     varchar2(1000);
BEGIN
  DBMS_LOB.createtemporary (lob_loc => l_data, cache   => TRUE, dur     => DBMS_LOB.call);
  l_conn := get_passive(p_conn);
  print_connection('локальный коннект на прием файла :', l_conn);
  dbms_output.put_line('cs_toftp(p_file)='||cs_toftp(p_file));
  send_command(p_conn, 'RETR ' || cs_toftp(p_file), TRUE);
  BEGIN
    LOOP
      l_amount := UTL_TCP.read_raw (l_conn, l_buffer, 32767);
      DBMS_LOB.writeappend(l_data, l_amount, l_buffer);
    END LOOP;
  EXCEPTION
    WHEN UTL_TCP.END_OF_INPUT THEN
      NULL;
    WHEN OTHERS THEN
      log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
  END;
  UTL_TCP.close_connection(l_conn);
  get_reply(p_conn); l_reply := g_reply(g_reply.last);
  log_ovart(-1,what_part,'получен двоичный файл "'||p_file||'" ('||l_reply||')');
  RETURN l_data;
END;



-- --------------------------------------------------------------------------
PROCEDURE put_local_ascii_data (p_data  IN  CLOB,
                                p_dir   IN  VARCHAR2,
                                p_file  IN  VARCHAR2) 
IS
  l_out_file  UTL_FILE.file_type;
  l_buffer    VARCHAR2(32767);
  l_amount    BINARY_INTEGER := 32767;
  l_pos       INTEGER := 1;
  l_clob_len  INTEGER;
BEGIN
  l_clob_len := DBMS_LOB.getlength(p_data);
  l_out_file := UTL_FILE.fopen(p_dir, p_file, 'w', 32767);
  WHILE l_pos < l_clob_len LOOP
    DBMS_LOB.read (p_data, l_amount, l_pos, l_buffer);
    IF g_convert_crlf THEN
      l_buffer := REPLACE(l_buffer, CHR(13), NULL);
    END IF;

    UTL_FILE.put(l_out_file, l_buffer);
    UTL_FILE.fflush(l_out_file);
    l_pos := l_pos + l_amount;
  END LOOP;

  UTL_FILE.fclose(l_out_file);
EXCEPTION
  WHEN OTHERS THEN
    IF UTL_FILE.is_open(l_out_file) THEN
      UTL_FILE.fclose(l_out_file);
    END IF;
    RAISE;
END;



-- --------------------------------------------------------------------------
PROCEDURE put_local_binary_data (p_data  IN  BLOB,
                                 p_dir   IN  VARCHAR2,
                                 p_file  IN  VARCHAR2) 
IS
  l_out_file  UTL_FILE.file_type;
  l_buffer    RAW(32767);
  l_amount    BINARY_INTEGER := 32767;
  l_pos       INTEGER := 1;
  l_blob_len  INTEGER;
BEGIN
  l_blob_len := DBMS_LOB.getlength(p_data);
  l_out_file := UTL_FILE.fopen(p_dir, p_file, 'w', 32767);
  WHILE l_pos < l_blob_len LOOP
    DBMS_LOB.read (p_data, l_amount, l_pos, l_buffer);
    UTL_FILE.put_raw(l_out_file, l_buffer, TRUE);
    UTL_FILE.fflush(l_out_file);
    l_pos := l_pos + l_amount;
  END LOOP;

  UTL_FILE.fclose(l_out_file);
EXCEPTION
  WHEN OTHERS THEN
    IF UTL_FILE.is_open(l_out_file) THEN
      UTL_FILE.fclose(l_out_file);
    END IF;
    RAISE;
END;

-- --------------------------------------------------------------------------
procedure print_connection(p_str varchar2, p_conn in out nocopy utl_tcp.connection)
is
begin
  debug(p_str||'. Remote_host='||p_conn.remote_host||':'||p_conn.remote_port ||'; '||
     'local_host='||p_conn.local_host||':'||p_conn.local_port ||'; private_sd='||p_conn.private_sd);
end;

-- --------------------------------------------------------------------------
PROCEDURE put_remote_ascii_data (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                                 p_file  IN             VARCHAR2,
                                 p_data  IN             CLOB,
                                 p_codepage in varchar2 default 'CL8MSWIN1251') 
IS
  l_conn      UTL_TCP.connection;
  l_result    PLS_INTEGER;
  l_buffer    VARCHAR2(32767);
  l_amount    BINARY_INTEGER := 32767;
  l_pos       INTEGER := 1;
  l_clob_len  INTEGER;
  l_reply     varchar2(1000);
  l_depth     number;
  l_file      varchar2(200);
BEGIN
  --log_ovart(-1,what_part,'------ p_file='||p_file||', p_data_size='||length(p_data)||', codepage='||p_codepage);
  l_conn := get_passive(p_conn);
  print_connection('локальный коннект на передачу файла :', l_conn);

  l_depth := CreatePath(p_conn, p_file);
  l_file:= replace(p_file ,'/','\');
  l_file:= substr(l_file, instr(l_file,'\',-1)+1);
  send_command(p_conn, gpv_storemethod || ' ' || l_file, TRUE);

  l_clob_len := DBMS_LOB.getlength(p_data);

  WHILE l_pos < l_clob_len LOOP
    DBMS_LOB.READ (p_data, l_amount, l_pos, l_buffer);
    IF g_convert_crlf THEN
      l_buffer := REPLACE(l_buffer, CHR(13), NULL);
    END IF;
    if p_codepage is not null then 
      l_buffer := convert(l_buffer,p_codepage);
    end if;
    l_result := UTL_TCP.write_text(l_conn, l_buffer, LENGTH(l_buffer));
    UTL_TCP.flush(l_conn);
    l_pos := l_pos + l_amount;
  END LOOP;

  UTL_TCP.close_connection(l_conn);
  
  get_reply(p_conn); l_reply := g_reply(g_reply.last);
  debug('l_reply='||l_reply);
  
  log_ovart(-1,what_part,'отправлен ascii файл "'||p_file||'" ('||l_reply||', '||l_pos||','||p_codepage||')');
  while l_depth>0
  loop
    debug('возвращаюсь на предыдущий каталог');
    ftp.send_command(p_conn,'CDUP', TRUE);        
    l_depth:=l_depth-1;
  end loop;
exception
  when others then 
    log_ovart(-1, what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
    log_ovart(-1, what_part, 'p_file='||p_file||', size='||length(p_data) );
END;



-- --------------------------------------------------------------------------
PROCEDURE put_remote_binary_data (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                                  p_file  IN             VARCHAR2,
                                  p_data  IN             BLOB) 
IS
  l_conn      UTL_TCP.connection;
  l_result    PLS_INTEGER;
  l_buffer    RAW(32767);
  l_amount    BINARY_INTEGER := 32767;
  l_pos       INTEGER := 1;
  l_blob_len  INTEGER;
  l_reply     varchar2(1000);
  l_depth     number;
  l_file      varchar2(200);
BEGIN
  debug('----------------- ');
  l_conn := get_passive(p_conn);
  print_connection('локальный коннект на передачу файла :', l_conn);

  l_depth := CreatePath(p_conn, p_file);
  l_file:= replace(p_file ,'/','\');
  l_file:= substr(l_file, instr(l_file,'\',-1)+1);
  send_command(p_conn, gpv_storemethod || ' ' || l_file, TRUE);

  l_blob_len := DBMS_LOB.getlength(p_data);

  WHILE l_pos < l_blob_len LOOP
    DBMS_LOB.READ (p_data, l_amount, l_pos, l_buffer);
    l_result := UTL_TCP.write_raw(l_conn, l_buffer, l_amount);
    UTL_TCP.flush(l_conn);
    l_pos := l_pos + l_amount;
  END LOOP;

  UTL_TCP.close_connection(l_conn);
  
  get_reply(p_conn); l_reply := g_reply(g_reply.last);
  debug('l_reply='||l_reply);
  
  log_ovart(-1,what_part,'отправил двоичный файл "'||p_file||'" ('||l_reply||', '||l_pos||')');
  while l_depth>0
  loop
    debug('возвращаюсь на предыдущий каталог');
    ftp.send_command(p_conn,'CDUP', TRUE);        
    l_depth:=l_depth-1;
  end loop;
exception
  when others then 
    log_ovart(-1, what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
    log_ovart(-1, what_part, 'p_file='||p_file||', size='||length(p_data) );
END;
-- --------------------------------------------------------------------------


/*
-- --------------------------------------------------------------------------
PROCEDURE get (p_conn       IN OUT NOCOPY  UTL_TCP.connection,
               p_from_file  IN             VARCHAR2,
               p_to_dir     IN             VARCHAR2,
               p_to_file    IN             VARCHAR2) AS
-- --------------------------------------------------------------------------
BEGIN
  IF g_binary THEN
    put_local_binary_data(p_data  => get_remote_binary_data (p_conn, p_from_file),
                          p_dir   => p_to_dir,
                          p_file  => p_to_file);
  ELSE
    put_local_ascii_data(p_data  => get_remote_ascii_data (p_conn, p_from_file),
                         p_dir   => p_to_dir,
                         p_file  => p_to_file);
  END IF;
END;
-- --------------------------------------------------------------------------



-- --------------------------------------------------------------------------
PROCEDURE put (p_conn       IN OUT NOCOPY  UTL_TCP.connection,
               p_from_dir   IN             VARCHAR2,
               p_from_file  IN             VARCHAR2,
               p_to_file    IN             VARCHAR2) 
AS
BEGIN
  IF g_binary THEN
    put_remote_binary_data(p_conn => p_conn,
                           p_file => p_to_file,
                           p_data => get_local_binary_data(p_from_dir, p_from_file));
  ELSE
    put_remote_ascii_data(p_conn => p_conn,
                          p_file => p_to_file,
                          p_data => get_local_ascii_data(p_from_dir, p_from_file));
  END IF;
  get_reply(p_conn);
END;



-- --------------------------------------------------------------------------
PROCEDURE get_direct (p_conn       IN OUT NOCOPY  UTL_TCP.connection,
                      p_from_file  IN             VARCHAR2,
                      p_to_dir     IN             VARCHAR2,
                      p_to_file    IN             VARCHAR2) 
IS
  l_conn        UTL_TCP.connection;
  l_out_file    UTL_FILE.file_type;
  l_amount      PLS_INTEGER;
  l_buffer      VARCHAR2(32767);
  l_raw_buffer  RAW(32767);
BEGIN
  l_conn := get_passive(p_conn);
  send_command(p_conn, 'RETR ' || p_from_file, TRUE);
  l_out_file := UTL_FILE.fopen(p_to_dir, p_to_file, 'w', 32767);

  BEGIN
    LOOP
      IF g_binary THEN
        l_amount := UTL_TCP.read_raw (l_conn, l_raw_buffer, 32767);
        UTL_FILE.put_raw(l_out_file, l_raw_buffer, TRUE);
      ELSE
        l_amount := UTL_TCP.read_text (l_conn, l_buffer, 32767);
        IF g_convert_crlf THEN
          l_buffer := REPLACE(l_buffer, CHR(13), NULL);
        END IF;
        UTL_FILE.put(l_out_file, l_buffer);
      END IF;
      UTL_FILE.fflush(l_out_file);
    END LOOP;
  EXCEPTION
    WHEN UTL_TCP.END_OF_INPUT THEN
      NULL;
    WHEN OTHERS THEN
      NULL;
  END;
  UTL_FILE.fclose(l_out_file);
  UTL_TCP.close_connection(l_conn);
EXCEPTION
  WHEN OTHERS THEN
    IF UTL_FILE.is_open(l_out_file) THEN
      UTL_FILE.fclose(l_out_file);
    END IF;
    RAISE;
END;


-- --------------------------------------------------------------------------
PROCEDURE put_direct (p_conn       IN OUT NOCOPY  UTL_TCP.connection,
                      p_from_dir   IN             VARCHAR2,
                      p_from_file  IN             VARCHAR2,
                      p_to_file    IN             VARCHAR2) IS
  l_conn        UTL_TCP.connection;
  l_bfile       BFILE;
  l_result      PLS_INTEGER;
  l_amount      PLS_INTEGER := 32767;
  l_raw_buffer  RAW(32767);
  l_len         NUMBER;
  l_pos         NUMBER := 1;
  ex_ascii      EXCEPTION;
BEGIN
  IF NOT g_binary THEN
    RAISE ex_ascii;
  END IF;

  l_conn := get_passive(p_conn);
  send_command(p_conn, 'STOR ' || p_to_file, TRUE);

  l_bfile := BFILENAME(p_from_dir, p_from_file);

  DBMS_LOB.fileopen(l_bfile, DBMS_LOB.file_readonly);
  l_len := DBMS_LOB.getlength(l_bfile);

  WHILE l_pos < l_len LOOP
    DBMS_LOB.READ (l_bfile, l_amount, l_pos, l_raw_buffer);
    debug(l_amount);
    l_result := UTL_TCP.write_raw(l_conn, l_raw_buffer, l_amount);
    l_pos := l_pos + l_amount;
  END LOOP;

  DBMS_LOB.fileclose(l_bfile);
  UTL_TCP.close_connection(l_conn);
EXCEPTION
  WHEN ex_ascii THEN
    RAISE_APPLICATION_ERROR(-20000, 'PUT_DIRECT not available in ASCII mode.');
  WHEN OTHERS THEN
    IF DBMS_LOB.fileisopen(l_bfile) = 1 THEN
      DBMS_LOB.fileclose(l_bfile);
    END IF;
    RAISE;
END;
*/
-- --------------------------------------------------------------------------
PROCEDURE help (p_conn  IN OUT NOCOPY  UTL_TCP.connection) AS
BEGIN
  send_command(p_conn, 'HELP', TRUE);
END;

-- --------------------------------------------------------------------------
PROCEDURE ascii (p_conn  IN OUT NOCOPY  UTL_TCP.connection) AS
BEGIN
  send_command(p_conn, 'TYPE A', TRUE);
  g_binary := FALSE;
END;

-- --------------------------------------------------------------------------
PROCEDURE binary (p_conn  IN OUT NOCOPY  UTL_TCP.connection) AS
BEGIN
  send_command(p_conn, 'TYPE I', TRUE);
  g_binary := TRUE;
END;

-- --------------------------------------------------------------------------
PROCEDURE convert_crlf (p_status  IN  BOOLEAN) AS
BEGIN
  g_convert_crlf := p_status;
END;

-- --------------------------------------------------------------------------
function GetSystem(p_conn  IN OUT NOCOPY  UTL_TCP.connection) return varchar2 AS
BEGIN
  send_command(p_conn, 'SYST', TRUE);
  l_ftpsystem := g_reply(g_reply.last);
  return g_reply(g_reply.last);
END;

-- --------------------------------------------------------------------------
PROCEDURE debug (p_text  IN  VARCHAR2) IS
BEGIN
  if gpv_debug then
    IF g_debug THEN
      DBMS_OUTPUT.put_line(SUBSTR(p_text, 1, 255));
    END IF;
    if l_sessionID = 0 then
      log_ovart(0,what_part,p_text);
    end if;
  end if;
END;

-- --------------------------------------------------------------------------
function CreatePath(p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                     p_path in varchar2) return number
is
  l_cnt number := 0;
  l_level number :=0;
  l_value varchar2(200):='';
begin
   for i in (SELECT regexp_substr(str, '[^\]+', 1, level) str
             FROM (SELECT p_path str FROM dual) t
             CONNECT BY instr(str, '\', 1, level - 1) > 0)
   loop
     if l_cnt>=1 and length(l_value)>0 then 
       begin
         ftp.send_command(p_conn,'CWD '||l_value, TRUE);        
       exception 
         when others then 
          begin
            dbms_output.put_line('создаю каталог '||l_value);
            ftp.send_command(p_conn,'MKD '||l_value, TRUE);        
            ftp.send_command(p_conn,'CWD '||l_value, TRUE);        
          exception 
            when others then 
              log_ovart(-1,what_part, 'attempt to make dir ('||l_value||') and change it to currect is fails ');
              log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
          end;
       end;
     end if;
     l_value := i.str;
     l_cnt := l_cnt + 1;
   end loop;
   l_level := l_cnt-1;
  
   return l_level;
exception
  when others then 
    log_ovart(-1, what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
    return 0;
end;

----------------------------------------------------------------------------
FUNCTION get_remote_file_list (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                               p_mask  IN             VARCHAR2 default null)
RETURN TStringList
IS
  l_conn    UTL_TCP.connection;
  l_amount  PLS_INTEGER;
  l_buffer  VARCHAR2(32767);
  l_data    CLOB;
  l_reply   varchar2(1000);
  l_ts      TStringList := TStringList();
  l_res     number;
BEGIN
  DBMS_LOB.createtemporary (lob_loc=> l_data, cache=> TRUE, dur=> DBMS_LOB.call);
  ftp.ascii(p_conn);
  l_conn := get_passive(p_conn);
  print_connection('локальный коннект на прием списка файлов :', l_conn);
  send_command(p_conn, 'NLST *', TRUE);
  BEGIN
    LOOP
      l_amount := UTL_TCP.read_text (l_conn, l_buffer, 32767);
      DBMS_LOB.writeappend(l_data, l_amount, cs_fromftp(l_buffer));
    END LOOP;
  EXCEPTION
    WHEN UTL_TCP.END_OF_INPUT THEN
      NULL;
    WHEN OTHERS THEN
      log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
  END;
  get_reply(p_conn); 
  l_reply := g_reply(g_reply.last);

  for i in (SELECT to_char(regexp_substr(str, '[^;]+', 1, level)) str
                FROM (SELECT replace(replace(l_data,chr(13),';'),chr(10),'') str FROM dual) t
              CONNECT BY instr(str, ';', 1, level - 1) > 0)
  loop
    if i.str is not null or length(i.str)>0 then 
      select count(1) into l_res from dual where upper(trim(i.str)) like upper(replace(nvl(p_mask,'%'),'*','%'));
      if l_res=1 then 
        l_ts.extend;l_ts(l_ts.count) := trim(i.str);
      end if;
    end if;
  end loop;

  UTL_TCP.close_connection(l_conn);
  RETURN l_ts;
exception
  when others then 
    log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
    return l_ts;
END;


----------------------------------------------------------------------------
FUNCTION get_file_MDMT (p_conn  IN OUT NOCOPY  UTL_TCP.connection,
                        p_file  IN             VARCHAR2 default null)
RETURN date
IS
  l_conn    UTL_TCP.connection;
  l_amount  PLS_INTEGER;
  l_buffer  VARCHAR2(32767);
  l_data    CLOB;
  l_reply   varchar2(1000);
  l_dat     date;
BEGIN
  DBMS_LOB.createtemporary (lob_loc=> l_data, cache=> TRUE, dur=> DBMS_LOB.call);
  ftp.ascii(p_conn);
  l_conn := get_passive(p_conn);
  send_command(p_conn, 'MDTM ' || p_file, TRUE);
  BEGIN
    LOOP
      l_amount := UTL_TCP.read_text (l_conn, l_buffer, 32767);
      DBMS_LOB.writeappend(l_data, l_amount, cs_fromftp(l_buffer));
    END LOOP;
  EXCEPTION
    WHEN UTL_TCP.END_OF_INPUT THEN
      NULL;
    WHEN OTHERS THEN
      log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
  END;
  get_reply(p_conn); 
  l_reply := g_reply(g_reply.last);

  debug(l_data);
  
  UTL_TCP.close_connection(l_conn);
  RETURN l_dat;
exception
  when others then 
    log_ovart(-1,what_part, dbms_utility.format_error_stack()||chr(13)||chr(10)||dbms_utility.format_error_backtrace());
    return l_dat;
END;


-- --------------------------------------------------------------------------
begin
  l_sessionID := sys_context('UserEnv','sessionID');
END;



-- End of DDL Script for Package Body SPARSHUKOV.FTP
/
