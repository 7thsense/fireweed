#define _GNU_SOURCE
#include <linux/perf_event.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
struct ring {int fd; char *map; unsigned long long tail;};
int main(int argc,char **argv) {
 if(argc<3)return 2;
 int gate[2];if(pipe(gate))return 2;
 pid_t child=fork(); if(child<0)return 2;
 if(child==0){close(gate[1]);char c;if(read(gate[0],&c,1)!=1)_exit(2);close(gate[0]);execvp(argv[2],argv+2);_exit(2);}
 close(gate[0]);
 int cpus=sysconf(_SC_NPROCESSORS_ONLN);struct ring *rings=calloc(cpus,sizeof(*rings));
 size_t page=sysconf(_SC_PAGESIZE),size=page*64;
 for(int i=0;i<cpus;i++){
  struct perf_event_attr a={0};a.size=sizeof(a);a.type=PERF_TYPE_SOFTWARE;a.config=PERF_COUNT_SW_CPU_CLOCK;
  a.disabled=1;a.inherit=1;a.exclude_kernel=1;a.exclude_hv=1;a.freq=1;a.sample_freq=199;a.sample_type=PERF_SAMPLE_IP;a.wakeup_events=1;
  rings[i].fd=syscall(__NR_perf_event_open,&a,child,i,-1,PERF_FLAG_FD_CLOEXEC);
  if(rings[i].fd<0){perror("perf_event_open");close(gate[1]);waitpid(child,0,0);return 2;}
  rings[i].map=mmap(0,page+size,PROT_READ|PROT_WRITE,MAP_SHARED,rings[i].fd,0);
  if(rings[i].map==MAP_FAILED){perror("mmap");close(gate[1]);waitpid(child,0,0);return 2;}
  ioctl(rings[i].fd,PERF_EVENT_IOC_ENABLE,0);
 }
 char path[512];snprintf(path,sizeof(path),"%s.samples",argv[1]);FILE *out=fopen(path,"w");if(!out){kill(child,SIGKILL);return 2;}
 write(gate[1],"x",1);close(gate[1]);
 unsigned long long count=0,lost=0;int status=0,done=0,ticks=0;
 do {
  for(int i=0;i<cpus;i++){
   struct perf_event_mmap_page *meta=(void*)rings[i].map;char *data=rings[i].map+page;unsigned long long tail=rings[i].tail;
   unsigned long long head=__atomic_load_n(&meta->data_head,__ATOMIC_ACQUIRE);
   while(tail<head){struct perf_event_header h;char buf[256];size_t off=tail%size,n=size-off;
    if(n>=sizeof(h))memcpy(&h,data+off,sizeof(h));else{memcpy(&h,data+off,n);memcpy((char*)&h+n,data,sizeof(h)-n);}
    if(h.size<sizeof(h)){kill(child,SIGKILL);return 2;}
    if(h.size<=sizeof(buf)){n=h.size;if(off+n<=size)memcpy(buf,data+off,n);else{memcpy(buf,data+off,size-off);memcpy(buf+size-off,data,n-(size-off));}
     if(h.type==PERF_RECORD_SAMPLE){uint64_t ip;memcpy(&ip,buf+sizeof(h),8);fprintf(out,"%llx\n",(unsigned long long)ip);count++;}
     if(h.type==PERF_RECORD_LOST){uint64_t v;memcpy(&v,buf+sizeof(h)+8,8);lost+=v;}
    }tail+=h.size;
   }
   rings[i].tail=tail;__atomic_store_n(&meta->data_tail,tail,__ATOMIC_RELEASE);
  }
  if(done)break;
  if(++ticks==20){char source[80];snprintf(source,sizeof(source),"/proc/%d/maps",child);FILE *in=fopen(source,"r");snprintf(path,sizeof(path),"%s.maps",argv[1]);FILE *m=fopen(path,"w");char b[4096];if(in&&m){size_t n;while((n=fread(b,1,sizeof(b),in)))fwrite(b,1,n,m);}if(in)fclose(in);if(m)fclose(m);}
  if(waitpid(child,&status,WNOHANG)==child){done=1;for(int i=0;i<cpus;i++)ioctl(rings[i].fd,PERF_EVENT_IOC_DISABLE,0);}else usleep(50000);
 }while(1);
 fclose(out);fprintf(stderr,"samples=%llu lost=%llu cpus=%d frequency=199\n",count,lost,cpus);
 return WIFEXITED(status)?WEXITSTATUS(status):128+WTERMSIG(status);
}
