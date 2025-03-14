-- test invalid connection request (random bytes) to ic proxy:
-- it should be dropped due to mismatch magic number
-- start_ignore
CREATE OR REPLACE LANGUAGE plpython3u;
-- end_ignore
CREATE OR REPLACE FUNCTION send_bytes_to_icproxy()
    RETURNS VOID as $$
import socket, struct, random

# parse host and port from gp_interconnect_proxy_addresses
icproxy_host = ""
icproxy_port = -1
try:
    res = plpy.execute("show gp_interconnect_type;", 1)
    if res[0]["gp_interconnect_type"] == "proxy":
        res = plpy.execute("show gp_interconnect_proxy_addresses;", 1)
        addrs = res[0]["gp_interconnect_proxy_addresses"]
        addr = addrs.split(",")[1]
        icproxy_host = addr.split(":")[2]
        icproxy_port = int(addr.split(":")[3])
except:
    # not working on icproxy mode, just return
    icproxy_host = ""
    pass
if icproxy_host == "":
    return

def ranstr(num):
    H = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    salt = ''
    for i in range(num):
        salt += random.choice(H)
    return salt

for i in range(10):
    # send the random bytes
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((icproxy_host, icproxy_port))
    val = struct.pack('!i', 134591) # an invalid magic number
    s.send(val)

    message_len = random.randint(100, 10000)
    message = ranstr(message_len).encode('utf-8')
    val = struct.pack('!i', message_len)
    s.send(val)
    s.sendall(message)
    s.close()

$$ LANGUAGE plpython3u;

create table PR_14998(i int);
SELECT send_bytes_to_icproxy();
-- the query hung here before the commit adding the magic number field in the ICProxyPkt
-- reason: random bytes cause icproxy OOM or hang forever
SET statement_timeout = 10000;
select count(*) from PR_14998;
RESET statement_timeout;
drop table PR_14998;
