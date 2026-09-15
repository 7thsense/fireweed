import os,time,mmap,tempfile,json,hashlib
from pathlib import Path
root=Path(tempfile.mkdtemp(prefix='local-storage-check-',dir='target'))
results=[]
block=os.urandom(1024*1024)
def stats(): return list(map(int,Path('/sys/block/nvme0n1/stat').read_text().split()))
try:
 for name,direct,size,sync_each,count in [('buffered-1MiB-final-sync',False,1048576,False,512),('direct-1MiB-final-sync',True,1048576,False,512),('buffered-4KiB-sync-each',False,4096,True,128),('buffered-1MiB-final-sync-repeat',False,1048576,False,512)]:
  path=root/name
  buf=mmap.mmap(-1,size);buf[:]=block[:size]
  fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_RDWR|(os.O_DIRECT if direct else 0),0o600)
  before=stats();start=time.monotonic();lat=[]
  for i in range(count):
   t=time.monotonic(); n=os.write(fd,buf)
   assert n==size
   if sync_each: os.fdatasync(fd)
   lat.append(time.monotonic()-t)
  writes_done=time.monotonic();os.fdatasync(fd);end=time.monotonic();after=stats();os.close(fd);buf.close()
  with path.open('rb') as f:
   assert f.read(size)==block[:size]; f.seek((count-1)*size);assert f.read(size)==block[:size]
  r=dict(name=name,bytes=size*count,seconds=end-start,write_loop_seconds=writes_done-start,final_sync_seconds=end-writes_done,MiB_per_second=size*count/(end-start)/1048576,operation_p50_ms=sorted(lat)[len(lat)//2]*1000,operation_max_ms=max(lat)*1000,device_written_MiB=(after[6]-before[6])*512/1048576,device_busy_seconds=(after[9]-before[9])/1000,verified_first_last=True)
  results.append(r);print(json.dumps(r),flush=True);path.unlink()
finally:
 root.rmdir()
 Path('/tmp/fireweed-local-storage-check.json').write_text(json.dumps(dict(note='Private fresh files on unchanged project Btrfs mount; repeated incompressible 1MiB buffer; device counters host-wide; no raw device writes or host tuning.',results=results),indent=2)+'\n')
