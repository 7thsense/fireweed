#define _GNU_SOURCE
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
struct worker { unsigned id, iterations; size_t bytes, count; int append, root; double *latencies; pthread_barrier_t *ready,*go; };
static double now_s(void) { struct timespec t; if(clock_gettime(CLOCK_MONOTONIC,&t)) { perror("clock");exit(2); } return t.tv_sec+t.tv_nsec/1e9; }
static void die(const char *operation) { perror(operation);_exit(2); }
static void write_all(int fd,const void *buf,size_t len) { const char *p=buf;while(len){ssize_t n=write(fd,p,len);if(n<0&&errno==EINTR)continue;if(n<=0)die("write");p+=n;len-=(size_t)n;} }
static void sync_fd(int fd,int data) { int r;do {r=data?fdatasync(fd):fsync(fd);}while(r&&errno==EINTR);if(r)die("sync"); }
static void measured_sync(struct worker *w,int fd,int data) { double t=now_s();sync_fd(fd,data);w->latencies[w->count++]=now_s()-t; }
static void publish(struct worker *w,int dir,unsigned iteration,const char *kind,const void *data,size_t len) {
 char temp[80],final[80];snprintf(temp,sizeof(temp),"%08u.%s.tmp",iteration,kind);snprintf(final,sizeof(final),"%08u.%s",iteration,kind);
 int fd=openat(dir,temp,O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);if(fd<0)die("open object");write_all(fd,data,len);measured_sync(w,fd,1);if(close(fd))die("close object");if(renameat(dir,temp,dir,final))die("rename");measured_sync(w,dir,0);
}
static void *run(void *arg) {
 struct worker *w=arg;char name[32];snprintf(name,sizeof(name),"thread-%03u",w->id);int dir=openat(w->root,name,O_RDONLY|O_DIRECTORY|O_CLOEXEC);if(dir<0)die("open directory");
 unsigned char *data=malloc(w->bytes),meta[4096];if(!data)die("malloc");uint64_t x=0x9e3779b97f4a7c15ULL+w->id;
 for(size_t i=0;i<w->bytes;i++){x^=x<<13;x^=x>>7;x^=x<<17;data[i]=(unsigned char)x;}memcpy(meta,data,sizeof(meta));
 int fd=-1;if(w->append){fd=openat(dir,"log.segment",O_WRONLY|O_CREAT|O_EXCL|O_APPEND|O_CLOEXEC,0600);if(fd<0)die("open segment");sync_fd(fd,1);sync_fd(dir,0);}
 pthread_barrier_wait(w->ready);pthread_barrier_wait(w->go);
 for(unsigned i=0;i<w->iterations;i++) {
  if(w->append){write_all(fd,data,w->bytes);measured_sync(w,fd,1);write_all(fd,meta,sizeof(meta));measured_sync(w,fd,1);}
  else {publish(w,dir,i,"data",data,w->bytes);publish(w,dir,i,"manifest",meta,sizeof(meta));}
 }
 if(w->append){struct stat st;if(fstat(fd,&st)||st.st_size!=(off_t)(w->iterations*(w->bytes+sizeof(meta))))die("segment size");if(close(fd))die("close segment");}
 else {struct stat st;snprintf(name,sizeof(name),"%08u.data",w->iterations-1);if(fstatat(dir,name,&st,0)||st.st_size!=(off_t)w->bytes)die("object size");}
 free(data);close(dir);return NULL;
}
static int cmp(const void *a,const void *b){double x=*(const double*)a,y=*(const double*)b;return (x>y)-(x<y);}
int main(int argc,char **argv){
 if(argc!=6){fprintf(stderr,"directory immutable|append threads iterations payload_bytes\n");return 2;}
 int append=!strcmp(argv[2],"append");if(!append&&strcmp(argv[2],"immutable"))return 2;
 unsigned threads=strtoul(argv[3],0,10),iterations=strtoul(argv[4],0,10);size_t bytes=strtoull(argv[5],0,10);if(!threads||threads>128||!iterations||iterations>4096||bytes<4096||bytes>4*1024*1024)return 2;
 int root=open(argv[1],O_RDONLY|O_DIRECTORY|O_CLOEXEC);if(root<0)die("open root");
 for(unsigned i=0;i<threads;i++){char name[32];snprintf(name,sizeof(name),"thread-%03u",i);if(mkdirat(root,name,0700))die("mkdir worker");}sync_fd(root,0);
 pthread_barrier_t ready,go;pthread_barrier_init(&ready,0,threads+1);pthread_barrier_init(&go,0,threads+1);
 struct worker *workers=calloc(threads,sizeof(*workers));pthread_t *ids=calloc(threads,sizeof(*ids));if(!workers||!ids)die("alloc workers");
 size_t capacity=(size_t)iterations*4;double setup=now_s();
 for(unsigned i=0;i<threads;i++){workers[i]=(struct worker){.id=i,.iterations=iterations,.bytes=bytes,.append=append,.root=root,.ready=&ready,.go=&go,.latencies=calloc(capacity,sizeof(double))};if(!workers[i].latencies||pthread_create(&ids[i],0,run,&workers[i]))die("start worker");}
 pthread_barrier_wait(&ready);setup=now_s()-setup;double start=now_s();pthread_barrier_wait(&go);for(unsigned i=0;i<threads;i++)if(pthread_join(ids[i],0))die("join");double elapsed=now_s()-start;
 double *latencies=malloc(threads*capacity*sizeof(double)),sum=0;size_t n=0;if(!latencies)die("alloc latencies");for(unsigned i=0;i<threads;i++){for(size_t j=0;j<workers[i].count;j++){double v=workers[i].latencies[j];latencies[n++]=v;sum+=v;}free(workers[i].latencies);}qsort(latencies,n,sizeof(double),cmp);
 unsigned long long total=(unsigned long long)threads*iterations*(bytes+4096);
 printf("{\"protocol\":\"%s\",\"prototype_cost_comparison_only\":true,\"threads\":%u,\"iterations_per_thread\":%u,\"data_bytes_per_iteration\":%zu,\"manifest_bytes_per_iteration\":4096,\"bytes\":%llu,\"setup_s\":%.9f,\"active_s\":%.9f,\"mib_s\":%.6f,\"timed_sync_calls\":%zu,\"setup_sync_calls\":%u,\"summed_overlapping_sync_s\":%.9f,\"sync_p50_s\":%.9f,\"sync_p95_s\":%.9f,\"sync_max_s\":%.9f}\n",argv[2],threads,iterations,bytes,total,setup,elapsed,total/1048576.0/elapsed,n,1+(append?threads*2:0),sum,latencies[(n-1)/2],latencies[(n-1)*95/100],latencies[n-1]);
 free(latencies);free(workers);free(ids);close(root);return 0;
}
