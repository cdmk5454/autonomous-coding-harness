"""Optional read-only context boundaries. No tool becomes an acceptance oracle."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path, PurePosixPath
from typing import Protocol, Callable
import math
import time
import os
import stat
from .core import (Binding, Scope, Mode, BoundaryError, require, text, number, integer,
                   hash64, canonical, digest)

@dataclass(frozen=True)
class ToolIdentity:
    name: str
    server_id: str
    version: str
    artifact_hash: str
    schema_hash: str
    host_attestation_ref: str
    def __post_init__(self):
        for x in (self.name,self.server_id,self.version,self.host_attestation_ref):text(x,maximum=1000)
        hash64(self.artifact_hash);hash64(self.schema_hash)
    @property
    def key(self):return digest(asdict(self))

class ReadOnlyTransport(Protocol):
    """Trusted HOST implementation, with enforced deadline/output bound.

    Server sandbox must deny Product writes/commit/publication; tool metadata or
    a model claiming readOnlyHint=true is NOT an independent attestation.
    """
    def attest(self, identity: ToolIdentity, binding: Binding, scope: Scope) -> bool: ...
    def call(self, tool: str, arguments: dict, *, timeout: float,max_bytes: int) -> dict: ...

SERENA_TOOLS={
    'find_symbol':frozenset(('name_path_pattern','relative_path','depth','include_body','include_kinds','exclude_kinds','substring_matching','max_answer_chars')),
    'find_referencing_symbols':frozenset(('name_path','relative_path','include_kinds','exclude_kinds','max_answer_chars')),
    'get_symbols_overview':frozenset(('relative_path','depth','max_answer_chars')),
}
CONTEXT7_TOOLS={'resolve-library-id':frozenset(('libraryName','query')),
                'query-docs':frozenset(('libraryId','query'))}
# Deliberately excludes evaluate_script, navigation, clicks, uploads, writes,
# downloads, profiler start, and any E2E acceptance assertions.
CHROME_TOOLS={'list_console_messages':frozenset(('pageSize','pageIdx','types','includePreservedMessages')),
              'list_network_requests':frozenset(('pageSize','pageIdx','resourceTypes','includePreservedRequests'))}


def relative_source(scope: Scope, value: str) -> str:
    text(value,'SOURCE_PATH_INVALID',1000)
    p=value.replace('\\','/')
    require(not p.startswith('/') and ':' not in p and all(x not in ('','.','..') for x in p.split('/'))
            and not any(c in p for c in '*?[]'),'SOURCE_PATH_INVALID')
    root=Path(scope.candidate_root)
    require((root/p).resolve().is_relative_to(root),'SOURCE_PATH_ESCAPE')
    return p


def _read_source_bytes_windows(scope: Scope, relative: str, limit: int) -> bytes:
    """Open a no-write handle and verify its final native path before reading."""
    import ctypes
    import msvcrt
    import ntpath
    root=Path(scope.candidate_root)
    target=root.joinpath(*relative.split('/'))
    directory=root
    for component in ('', *relative.split('/')[:-1]):
        if component:directory=directory/component
        st=directory.lstat()
        require(stat.S_ISDIR(st.st_mode) and not getattr(st,'st_file_attributes',0)&0x400,
                'SOURCE_PATH_ESCAPE')
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    create=kernel.CreateFileW
    create.argtypes=(ctypes.c_wchar_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p,
                     ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p)
    create.restype=ctypes.c_void_p
    handle=create(str(target),0x80000000,1,None,3,0x00200000,None)
    require(handle!=ctypes.c_void_p(-1).value,'CONTEXT_SECURE_OPEN_FAILED')
    fd=None
    try:
        final=kernel.GetFinalPathNameByHandleW
        final.argtypes=(ctypes.c_void_p,ctypes.c_wchar_p,ctypes.c_uint32,ctypes.c_uint32)
        final.restype=ctypes.c_uint32
        buffer=ctypes.create_unicode_buffer(32768)
        length=final(ctypes.c_void_p(handle),buffer,len(buffer),0)
        require(0<length<len(buffer),'CONTEXT_SECURE_OPEN_FAILED')
        observed=buffer.value
        if observed.startswith('\\\\?\\'):observed=observed[4:]
        expected=ntpath.normcase(ntpath.normpath(str(root.resolve()/relative)))
        require(ntpath.normcase(ntpath.normpath(observed))==expected,'SOURCE_PATH_ESCAPE')
        st=target.lstat()
        require(stat.S_ISREG(st.st_mode) and not getattr(st,'st_file_attributes',0)&0x400,
                'CONTEXT_REGULAR_FILE_REQUIRED')
        fd=msvcrt.open_osfhandle(handle,os.O_RDONLY|os.O_BINARY)
        handle=None
        opened=os.fstat(fd)
        require(stat.S_ISREG(opened.st_mode) and opened.st_size==st.st_size
                and opened.st_ino==st.st_ino and opened.st_dev==st.st_dev,
                'CONTEXT_SOURCE_CHANGED')
        result=bytearray()
        while len(result)<=limit:
            part=os.read(fd,min(65536,limit+1-len(result)))
            if not part:break
            result.extend(part)
        require(len(result)<=limit,'CONTEXT_FILE_TOO_LARGE')
        after=target.lstat()
        require(after.st_ino==st.st_ino and after.st_dev==st.st_dev
                and after.st_size==st.st_size and after.st_mtime_ns==st.st_mtime_ns,
                'CONTEXT_SOURCE_CHANGED')
        return bytes(result)
    finally:
        if fd is not None:os.close(fd)
        elif handle is not None:kernel.CloseHandle(ctypes.c_void_p(handle))

def read_source_bytes(scope: Scope, relative: str, limit: int) -> bytes:
    """Descriptor-relative no-follow traversal. Unsupported host => safe HOLD.

    The Windows host bridge must provide an equivalent handle-based reader
    before native promotion; do not substitute unchecked Path.read_bytes().
    """
    relative=relative_source(scope,relative)
    denied={'.git','.ssh','.env','auth.json','credentials.json','id_rsa','id_ed25519'}
    for component in relative.split('/'):
        low=component.casefold()
        require(low not in denied and not low.startswith('.env.') and not low.endswith(('.pem','.key','.p12','.pfx')),'CONTEXT_SENSITIVE_PATH_DENIED')
    if os.name=='nt':
        return _read_source_bytes_windows(scope,relative,limit)
    require(hasattr(os,'O_NOFOLLOW') and os.open in os.supports_dir_fd,'CONTEXT_SECURE_OPEN_UNSUPPORTED')
    flags=os.O_RDONLY | os.O_NOFOLLOW
    directory_flag=getattr(os,'O_DIRECTORY',0)
    root_fd=os.open(scope.candidate_root,flags|directory_flag)
    current=root_fd;opened=[]
    try:
        pieces=relative.split('/')
        for component in pieces[:-1]:
            fd=os.open(component,flags|directory_flag,dir_fd=current);opened.append(fd);current=fd
        fd=os.open(pieces[-1],flags|getattr(os,'O_NONBLOCK',0),dir_fd=current);opened.append(fd)
        require(stat.S_ISREG(os.fstat(fd).st_mode),'CONTEXT_REGULAR_FILE_REQUIRED')
        result=bytearray()
        while len(result)<=limit:
            part=os.read(fd,min(65536,limit+1-len(result)))
            if not part:break
            result.extend(part)
        require(len(result)<=limit,'CONTEXT_FILE_TOO_LARGE')
        return bytes(result)
    finally:
        for fd in reversed(opened):os.close(fd)
        os.close(root_fd)

class ContextBoundary:
    def __init__(self, kind: str, *, mode: Mode=Mode.OFF, identity: ToolIdentity | None=None,
                 transport: ReadOnlyTransport | None=None, timeout_seconds=5.0,
                 max_bytes=65536, clock=time.monotonic):
        require(kind in ('serena','context7','chrome_devtools'),'CONTEXT_KIND_INVALID')
        require(mode in (Mode.OFF,Mode.SHADOW),'CONTEXT_AUTHORITY_UNSUPPORTED')
        number(timeout_seconds,.01,60);integer(max_bytes,256,262144)
        self.kind=kind;self.mode=mode;self.identity=identity;self.transport=transport
        self.timeout=timeout_seconds;self.limit=max_bytes;self.clock=clock
    def _arguments(self,scope,tool,arguments):
        table={'serena':SERENA_TOOLS,'context7':CONTEXT7_TOOLS,'chrome_devtools':CHROME_TOOLS}[self.kind]
        require(tool in table,'MCP_CAPABILITY_DENIED')
        require(type(arguments) is dict and set(arguments).issubset(table[tool]) and len(canonical(arguments))<=16384,'MCP_ARGUMENTS_DENIED')
        args=dict(arguments)
        if self.kind=='serena':
            if 'relative_path' in args:args['relative_path']=relative_source(scope,args['relative_path'])
            if tool in ('find_referencing_symbols','get_symbols_overview'):require('relative_path' in args,'MCP_PATH_REQUIRED')
            for key in ('name_path','name_path_pattern'):
                if key in args:text(args[key],'MCP_ARGUMENTS_DENIED',500)
            if tool=='find_symbol':require('name_path_pattern' in args,'MCP_SYMBOL_REQUIRED')
            if tool=='find_referencing_symbols':require('name_path' in args,'MCP_SYMBOL_REQUIRED')
            for key in ('include_body','substring_matching'):
                if key in args:require(type(args[key]) is bool,'MCP_ARGUMENTS_DENIED')
            for key in ('include_kinds','exclude_kinds'):
                if key in args:
                    require(type(args[key]) is list and len(args[key])<=30,'MCP_ARGUMENTS_DENIED')
                    for x in args[key]:integer(x,1,26,'MCP_ARGUMENTS_DENIED')
            if 'depth' in args:integer(args['depth'],0,5,'MCP_ARGUMENTS_DENIED')
            if 'max_answer_chars' in args:integer(args['max_answer_chars'],1,self.limit//4,'MCP_ARGUMENTS_DENIED')
            else:args['max_answer_chars']=self.limit//4
        elif self.kind=='context7':
            require(set(args)==table[tool],'MCP_ARGUMENTS_DENIED')
            for v in args.values():text(v,'MCP_ARGUMENTS_DENIED',4000)
            # Data classification/approval happens before external-doc requests;
            # this boundary cannot infer whether text contains proprietary code.
        else:
            for k in ('pageSize','pageIdx'):
                if k in args:integer(args[k],0,100,'MCP_ARGUMENTS_DENIED')
            if 'includePreservedMessages' in args:require(type(args['includePreservedMessages']) is bool,'MCP_ARGUMENTS_DENIED')
            if 'includePreservedRequests' in args:require(type(args['includePreservedRequests']) is bool,'MCP_ARGUMENTS_DENIED')
            for k in ('types','resourceTypes'):
                if k in args:
                    require(type(args[k]) is list and len(args[k])<=30,'MCP_ARGUMENTS_DENIED')
                    for v in args[k]:text(v,'MCP_ARGUMENTS_DENIED',100)
        return args
    def query(self,binding: Binding,scope: Scope,tool: str,arguments: dict, *,
              external_data_approved: bool=False,fallback: Callable[[],dict] | None=None,
              relevance_verifier: Callable[[dict],bool] | None=None) -> dict:
        require(isinstance(binding,Binding) and isinstance(scope,Scope),'CONTEXT_BINDING_REQUIRED')
        result={'kind':self.kind,'binding_key':binding.key,'status':'OFF','content':None,
                'lookup_seconds':0.0,'time_to_first_relevant_source':None,'files_opened':None,
                'context_tokens':None,'wrong_path_exploration_proxy':None,'accepted_result_link':None,
                'product_pass':False,'publication_authority':False,'e2e_acceptance_authority':False}
        started=self.clock()
        try:
            if self.mode==Mode.OFF:return result
            args=self._arguments(scope,tool,arguments)
            require(external_data_approved is True,'CONTEXT_DATA_NOT_APPROVED')
            require(isinstance(self.identity,ToolIdentity) and self.identity.name==self.kind and self.transport is not None,'MCP_UNAVAILABLE')
            require(self.transport.attest(self.identity,binding,scope) is True,'MCP_READONLY_ATTESTATION_REQUIRED')
            response=self.transport.call(tool,args,timeout=self.timeout,max_bytes=self.limit)
            require(self.clock()-started<=self.timeout,'MCP_TIMEOUT')
            require(type(response) is dict and len(canonical(response))<=self.limit,'MCP_RESPONSE_TOO_LARGE_OR_INVALID')
            require(response.get('isError') is False,'MCP_TOOL_FAILED')
            require(set(response).issubset({'isError','content','host_metrics'}),'MCP_SCHEMA_INVALID')
            # Treat server content as untrusted, NOT instructions. No downstream
            # execution. Metrics only from separately attested HOST collection.
            content=response.get('content')
            require(type(content) in (dict,list,str),'MCP_SCHEMA_INVALID')
            result.update(status='READ_ONLY_OBSERVATION',content=content,tool=tool,
                          tool_identity=asdict(self.identity),argument_hash=digest(args),response_hash=digest(content))
            metrics=response.get('host_metrics')
            if metrics is not None:
                require(type(metrics) is dict and set(metrics).issubset({'files_opened','context_tokens','wrong_path_exploration_proxy'}),'MCP_METRICS_INVALID')
                for key,value in metrics.items():integer(value);result[key]=value
            if relevance_verifier is not None and relevance_verifier(result) is True:
                result['time_to_first_relevant_source']=max(0,self.clock()-started)
        except BoundaryError as exc:result.update(status='SAFE_FALLBACK',content=None,error_code=exc.code)
        except TimeoutError:result.update(status='SAFE_FALLBACK',content=None,error_code='MCP_TIMEOUT')
        except Exception:result.update(status='SAFE_FALLBACK',content=None,error_code='MCP_UNAVAILABLE_OR_INVALID')
        finally:result['lookup_seconds']=max(0,self.clock()-started)
        if result['status']=='SAFE_FALLBACK' and fallback is not None:
            try:
                existing=fallback()
                require(type(existing) is dict and len(canonical(existing))<=self.limit,'SOURCE_SEARCH_INVALID')
                result['fallback_source_search']=existing
            except Exception:result['fallback_error_code']='SOURCE_SEARCH_UNAVAILABLE'
        return result

class ContextBudget:
    """Repomix is optional. byte budget is hard, token estimate is labeled.

    Native packager MUST be a host bounded read-only invocation and return only
    text. No arbitrary command string is accepted. No silent partial truncation.
    """
    def __init__(self, *, byte_budget: int,token_budget: int,mode: Mode=Mode.OFF,
                 tokenizer: Callable[[str],int] | None=None,tokenizer_ref: str | None=None):
        integer(byte_budget,1,16_777_216);integer(token_budget,1,16_777_216)
        require(mode in (Mode.OFF,Mode.SHADOW),'CONTEXT_BUDGET_MODE_INVALID')
        require((tokenizer is None)==(tokenizer_ref is None),'TOKENIZER_PROVENANCE_REQUIRED')
        if tokenizer_ref is not None:text(tokenizer_ref,maximum=1000)
        self.bytes=byte_budget;self.tokens=token_budget;self.mode=mode
        self.tokenizer=tokenizer;self.tokenizer_ref=tokenizer_ref
    def _measure(self,value):
        size=len(value.encode('utf-8'))
        count=self.tokenizer(value) if self.tokenizer else math.ceil(size/3)
        integer(count,0,2**40,'TOKEN_COUNT_INVALID')
        return {'bytes':size,'tokens':count,'token_measurement':self.tokenizer_ref or 'ESTIMATE_UTF8_BYTES_DIV_3_NOT_EXACT'}
    def package(self,binding: Binding,scope: Scope,files: tuple[str,...], *, packager=None,
                package_identity: ToolIdentity | None=None,timeout_seconds=10) -> dict:
        require(type(files) is tuple and 1<=len(files)<=256 and len(set(files))==len(files),'CONTEXT_FILES_INVALID')
        number(timeout_seconds,.01,60)
        result={'binding_key':binding.key,'status':'OFF','context':None,'before':None,'after':None,
                'packager_used':False,'product_pass':False,'publication_authority':False}
        if self.mode==Mode.OFF:return result
        try:
            # Always bound input *before* invoking a tokenizer or external tool.
            parts=[];total=0
            for name in files:
                rel=relative_source(scope,name)
                raw=read_source_bytes(scope,rel,1_048_576)
                total+=len(raw);require(total<=8_388_608,'CONTEXT_INPUT_TOO_LARGE')
                body=raw.decode('utf-8')
                parts.append({'path':rel,'content':body})
            original=canonical({'files':parts}).decode('utf-8')
            result['before']=self._measure(original);context=original
            if packager is not None:
                try:
                    require(isinstance(package_identity,ToolIdentity) and package_identity.name=='repomix','REPOMIX_IDENTITY_REQUIRED')
                    # Pass approved in-memory input, not a broad workspace glob.
                    started=time.monotonic()
                    candidate=packager(tuple(parts),timeout=timeout_seconds,max_bytes=self.bytes)
                    require(time.monotonic()-started<=timeout_seconds,'REPOMIX_TIMEOUT')
                    require(type(candidate) is str and len(candidate.encode('utf-8'))<=self.bytes,'REPOMIX_OUTPUT_INVALID')
                    context=candidate;result['packager_used']=True;result['identity']=asdict(package_identity)
                    result['lossless_verified']=False
                except BoundaryError as exc:result['packager_error_code']=exc.code
                except TimeoutError:result['packager_error_code']='REPOMIX_TIMEOUT'
                except Exception:result['packager_error_code']='REPOMIX_UNAVAILABLE'
                # An optional tool failure never discards an already safe built-in
                # context. The unchanged original still must pass both budgets.
            result['after']=self._measure(context)
            require(result['after']['bytes']<=self.bytes,'CONTEXT_BYTE_BUDGET_EXCEEDED')
            require(result['after']['tokens']<=self.tokens,'CONTEXT_TOKEN_BUDGET_EXCEEDED')
            result.update(status='CONTEXT_BUDGET_ACCEPTED',context=context,context_hash=digest(context),
                          token_budget_exact=self.tokenizer is not None)
        except BoundaryError as exc:result.update(status='BUDGET_OR_PACKAGER_HOLD',error_code=exc.code,context=None)
        except TimeoutError:result.update(status='BUDGET_OR_PACKAGER_HOLD',error_code='REPOMIX_TIMEOUT',context=None)
        except Exception:result.update(status='BUDGET_OR_PACKAGER_HOLD',error_code='REPOMIX_OR_SOURCE_UNAVAILABLE',context=None)
        return result
