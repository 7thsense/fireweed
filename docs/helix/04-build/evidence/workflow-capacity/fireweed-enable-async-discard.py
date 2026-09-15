from pathlib import Path
import shutil,subprocess,time,os
assert os.geteuid()==0
p=Path('/etc/fstab');old=p.read_text();lines=[];count=0
for line in old.splitlines(keepends=True):
 fields=line.split()
 if fields and not line.lstrip().startswith('#') and len(fields)>=4 and fields[0]=='UUID=c3a024c4-06da-4716-add4-543fcb50fa70' and fields[2]=='btrfs':
  options=fields[3].split(',')
  assert not any(x.startswith('discard') or x=='nodiscard' for x in options), 'Unexpected discard setting'
  line=line.replace(fields[3],fields[3]+',discard=async',1);count+=1
 lines.append(line)
assert count==4,count
backup=Path('/etc/fstab.fireweed-before-async-'+str(int(time.time())))
shutil.copy2(p,backup)
p.write_text(''.join(lines))
subprocess.run(['findmnt','--verify','--tab-file','/etc/fstab'],check=True)
subprocess.run(['systemctl','daemon-reload'],check=True)
subprocess.run(['mount','-o','remount,discard=async','/'],check=True)
result=subprocess.check_output(['findmnt','-T','/home','-o','TARGET,SOURCE,FSTYPE,OPTIONS']).decode()
assert 'discard=async' in result,result
Path('/tmp/fireweed-trim-recovery/async-discard.txt').write_text('Backup: '+str(backup)+'\n'+result)
print(result,flush=True)
