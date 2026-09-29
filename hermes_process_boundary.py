import ctypes as c, ctypes.wintypes as w, subprocess, _winapi, time, os, msvcrt
k=c.WinDLL('kernel32',use_last_error=True)
def setup(name,restype,args):
 f=getattr(k,name); f.restype=restype; f.argtypes=args; return f
CreateJob=setup('CreateJobObjectW',w.HANDLE,[c.c_void_p,w.LPCWSTR]); SetJob=setup('SetInformationJobObject',w.BOOL,[w.HANDLE,c.c_int,c.c_void_p,w.DWORD]); Assign=setup('AssignProcessToJobObject',w.BOOL,[w.HANDLE,w.HANDLE]); Query=setup('QueryInformationJobObject',w.BOOL,[w.HANDLE,c.c_int,c.c_void_p,w.DWORD,c.c_void_p]); Close=setup('CloseHandle',w.BOOL,[w.HANDLE]); Open=setup('OpenProcess',w.HANDLE,[w.DWORD,w.BOOL,w.DWORD]); Times=setup('GetProcessTimes',w.BOOL,[w.HANDLE,c.c_void_p,c.c_void_p,c.c_void_p,c.c_void_p]); Resume=setup('ResumeThread',w.DWORD,[w.HANDLE]); Wait=setup('WaitForSingleObject',w.DWORD,[w.HANDLE,w.DWORD]); Kill=setup('TerminateProcess',w.BOOL,[w.HANDLE,w.UINT])
class Basic(c.Structure):
 _fields_=[('user',c.c_longlong),('kernel',c.c_longlong),('flags',w.DWORD),('min_ws',c.c_size_t),('max_ws',c.c_size_t),('active',w.DWORD),('affinity',c.c_size_t),('priority',w.DWORD),('scheduling',w.DWORD)]
class Extended(c.Structure):
 _fields_=[('basic',Basic),('io',c.c_ulonglong*6),('pm',c.c_size_t),('jm',c.c_size_t),('peakpm',c.c_size_t),('peakjm',c.c_size_t)]
def check(v):
 if not v: raise c.WinError(c.get_last_error())
def identity(pid):
 h=Open(0x101000,False,pid)
 if not h:return {'pid':pid,'accessible':False,'error_code':c.get_last_error()}
 try:
  t=[c.c_ulonglong() for _ in range(4)]; check(Times(h,*[c.byref(x) for x in t]))
  state=Wait(h,0)
  if state not in (0,258):raise c.WinError(c.get_last_error())
  return {'pid':pid,'creation_time_100ns':t[0].value,'alive':state==258}
 finally:Close(h)
class OwnedProcess:
 def __init__(self,exe,args,env,cwd,log,*,stderr=None,stdin=None):
  self.job=None; self.handle=None; self.null=None; ht=None; inherited={}
  try:
   self.job=CreateJob(None,None); check(self.job); limits=Extended(); limits.basic.flags=0x2000; check(SetJob(self.job,9,c.byref(limits),c.sizeof(limits)))
   si=subprocess.STARTUPINFO(); si.dwFlags=0x100; self.null=open(os.devnull,'rb') if stdin is None else stdin; self.owns_input=stdin is None
   error_log=stderr if stderr is not None else log
   for f in (self.null,log,error_log):
    h=msvcrt.get_osfhandle(f.fileno())
    if h not in inherited:inherited[h]=os.get_handle_inheritable(h)
    os.set_handle_inheritable(h,True)
   si.hStdInput=msvcrt.get_osfhandle(self.null.fileno()); si.hStdOutput=msvcrt.get_osfhandle(log.fileno()); si.hStdError=msvcrt.get_osfhandle(error_log.fileno())
   # Inherit only this role's stdio, never unrelated inheritable handles.
   si.lpAttributeList={'handle_list':sorted(inherited)}
   hp,ht,pid,tid=_winapi.CreateProcess(exe,subprocess.list2cmdline(args),None,None,True,0x08000000|0x4,env,cwd,si)
   self.handle=hp; self.pid=pid
   check(Assign(self.job,hp)); check(Resume(ht)!=0xffffffff)
  except Exception:
   if self.handle:Kill(self.handle,90);Wait(self.handle,5000);_winapi.CloseHandle(self.handle);self.handle=None
   if self.job:Close(self.job);self.job=None
   raise
  finally:
   if ht is not None:_winapi.CloseHandle(ht)
   for h,was_inheritable in inherited.items():os.set_handle_inheritable(h,was_inheritable)
   if self.handle is None and self.null is not None and self.owns_input:self.null.close()
 def poll(self):
  state=Wait(self.handle,0)
  if state==258:return None
  if state!=0:raise c.WinError(c.get_last_error())
  return _winapi.GetExitCodeProcess(self.handle)
 def members(self):
  if not self.job:return []
  buf=c.create_string_buffer(8+8*2048); check(Query(self.job,3,buf,len(buf),None)); n=c.c_uint32.from_buffer(buf,4).value
  return [int(c.c_size_t.from_buffer(buf,8+i*c.sizeof(c.c_size_t)).value) for i in range(n)]
 def close_boundary(self):
  ids=[identity(p) for p in self.members()]
  if self.job: check(Close(self.job)); self.job=None
  return ids
 def terminate(self): self.close_boundary()
 def terminate_and_wait(self,timeout=5):
  """Terminate only this owned Job and prove all its members have exited."""
  terminate_job=setup('TerminateJobObject',w.BOOL,[w.HANDLE,w.UINT])
  check(terminate_job(self.job,90)); deadline=time.monotonic()+timeout
  while self.members():
   if time.monotonic()>=deadline:raise TimeoutError('owned job quiescence')
   time.sleep(.01)
  self.wait(timeout=max(.001,deadline-time.monotonic()))
 def release(self):
  """Release resources after exit/quiescence has been established."""
  if self.job:self.close_boundary()
  if self.handle:_winapi.CloseHandle(self.handle);self.handle=None
  if self.owns_input:self.null.close()
 def wait(self,timeout=20):
  if type(timeout) not in (int,float) or not __import__('math').isfinite(timeout) or timeout < 0:
   raise ValueError('owned process timeout invalid')
  state=Wait(self.handle,min(0xfffffffe,int(timeout*1000)))
  if state==258:raise TimeoutError('owned process exit')
  if state!=0:raise c.WinError(c.get_last_error())
  return _winapi.GetExitCodeProcess(self.handle)
 def crash_pid(self,pid):
  if pid not in self.members():raise ValueError('not owned')
  h=Open(0x1001,False,pid); check(h); check(Kill(h,91)); Close(h)
