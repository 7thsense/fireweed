import sys,bisect,collections,struct,json,gzip,hashlib
from pathlib import Path
prefix=sys.argv[1]
def read_text(path):
 p=Path(path)
 return p.read_text() if p.exists() else gzip.decompress(Path(str(p)+'.gz').read_bytes()).decode()
provenance=json.loads(read_text(prefix+'-provenance.json'))
ranges=[]
for line in read_text(prefix+'.maps').splitlines():
 p=line.split();a,b=(int(x,16) for x in p[0].split('-'));ranges.append((a,b,int(p[2],16),p[-1] if len(p)>5 else '[anonymous]'))
segments=[(s['file_offset']//4096*4096,s['file_offset']+s['file_size'],s['virtual_address']-s['file_offset']) for s in provenance['elf_load_segments']]
def virtual_offset(off):
 return next(off+delta for start,end,delta in segments if start<=off<end)
syms=[]
for line in read_text(prefix+'.symbols').splitlines():
 p=line.split(maxsplit=2)
 if len(p)==3 and p[1].lower() in ('t','w'):syms.append((int(p[0],16),p[2]))
libsyms=[]
libpath=Path(sys.argv[2]) if len(sys.argv)>2 else Path(prefix).parent/'fireweed-9500-libc-private.symbols'
libtext=read_text(libpath)
assert hashlib.sha256(libtext.encode()).hexdigest()==provenance['libc_private_symbols_sha256']
for line in libtext.splitlines():
 p=line.split(maxsplit=2);libsyms.append((int(p[0],16),p[2]))
libsyms.sort();libaddr=[a for a,b in libsyms]
syms.sort();addresses=[s[0] for s in syms];counts=collections.Counter();total=0
for line in read_text(prefix+'.samples').splitlines():
 try:ip=int(line,16)
 except ValueError:continue
 total+=1
 for a,b,offset,path in ranges:
  if a<=ip<b:
   if path.endswith('/fireweed-workload'):
    i=bisect.bisect_right(addresses,virtual_offset(ip-a+offset))-1
    name=syms[i][1] if i>=0 else '[unknown executable]'
   elif path.endswith("/libc.so.6"):
    i=bisect.bisect_right(libaddr,ip-a+offset)-1;name="libc::"+libsyms[i][1]
   else:name=path
   counts[name]+=1;break
 else:counts['[unmapped]']+=1
import json
Path(prefix+'.counts.json').write_text(json.dumps({'total_samples':total,'leaf_symbols':counts},indent=2)+'\n')
print('total',total)
for name,n in counts.most_common(35):print(f'{100*n/max(1,total):6.2f}% {n:7d} {name}')
