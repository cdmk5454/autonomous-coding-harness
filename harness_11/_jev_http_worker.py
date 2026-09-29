"""Private fixed-origin bounded HTTP child. Execute only through JevAdapter.
No CLI flags, secrets in env/argv, redirects, proxy auto-discovery, or retries.
"""
import http.client
import json
import socket
import sys
import time

def main():
    connection=None
    try:
        raw=sys.stdin.buffer.read(400000)
        request=json.loads(raw)
        limit=request['limit']; timeout=request['timeout']
        if type(limit) is not int or not 256<=limit<=1048576: raise ValueError()
        if type(timeout) not in (int,float) or not .05<=timeout<=60: raise ValueError()
        body=request['body'].encode('utf-8'); secret=request['secret']
        if len(body)>262144 or not isinstance(secret,str) or not 0<len(secret)<=16384 or '\r' in secret or '\n' in secret: raise ValueError()
        deadline=time.monotonic()+timeout
        connection=http.client.HTTPSConnection('api.typesafe.ai',443,timeout=timeout)
        connection.request('POST','/v1/systemone',body=body,
                           headers={'Authorization':'Bearer '+secret,'Content-Type':'application/json','Accept':'application/json'})
        response=connection.getresponse()
        if response.status!=200:
            # Deliberately never echo service error bodies or follow redirects.
            answer={'status':response.status,'body':''}
        else:
            chunks=[]; size=0
            while True:
                remaining=deadline-time.monotonic()
                if remaining<=0: raise TimeoutError()
                if connection.sock is not None: connection.sock.settimeout(remaining)
                chunk=response.read1(min(8192,limit+1-size))
                if not chunk: break
                size+=len(chunk)
                if size>limit:
                    print('{"error":"JEV_RESPONSE_TOO_LARGE"}'); return
                chunks.append(chunk)
            answer={'status':response.status,'body':b''.join(chunks).decode('utf-8')}
        print(json.dumps(answer,ensure_ascii=True,separators=(',',':'),allow_nan=False))
    except (TimeoutError,socket.timeout):
        print('{"error":"JEV_TIMEOUT"}')
    except Exception:
        print('{"error":"JEV_NETWORK_ERROR"}')
    finally:
        if connection is not None: connection.close()

if __name__=='__main__': main()
