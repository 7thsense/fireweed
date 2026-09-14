#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

/* Diagnostic only: forward libc calls unchanged; record >=100ms calls.
 * Ordinary write/writev and asynchronous I/O are deliberately not intercepted.
 * Durations are caller wall time, never physical-device service time. */
static pthread_once_t initialized = PTHREAD_ONCE_INIT;
static ssize_t (*next_pwrite)(int,const void *,size_t,off_t);
static ssize_t (*next_pwritev)(int,const struct iovec *,int,off_t);
static int (*next_fsync)(int), (*next_fdatasync)(int);
static uint64_t threshold_us=100000;
static int trace_fd=STDERR_FILENO;
static void initialize(void) {
    *(void **)(&next_pwrite)=dlsym(RTLD_NEXT,"pwrite");
    *(void **)(&next_pwritev)=dlsym(RTLD_NEXT,"pwritev");
    *(void **)(&next_fsync)=dlsym(RTLD_NEXT,"fsync");
    *(void **)(&next_fdatasync)=dlsym(RTLD_NEXT,"fdatasync");
    if (!next_pwrite || !next_pwritev || !next_fsync || !next_fdatasync) _exit(125);
    const char *s=getenv("FIREWEED_SLOW_IO_US");
    if (s) threshold_us=strtoull(s,NULL,10);
    const char *path=getenv("FIREWEED_SLOW_IO_PATH");
    if (path) {
        trace_fd=open(path,O_WRONLY|O_CREAT|O_APPEND|O_CLOEXEC,0600);
        if (trace_fd<0) _exit(126);
    }
}
static uint64_t micros(clockid_t clock) {
    struct timespec t;
    if (clock_gettime(clock,&t)) return 0;
    return (uint64_t)t.tv_sec*1000000+(uint64_t)t.tv_nsec/1000;
}
static void report(const char *op,int fd,uint64_t bytes,off_t offset,
                   int64_t result,int saved_errno,uint64_t started) {
    uint64_t ended=micros(CLOCK_MONOTONIC), elapsed=ended-started;
    if (elapsed<threshold_us) return;
    uint64_t unix_end=micros(CLOCK_REALTIME);
    char link[64],path[1024],escaped[2100],line[2600];
    snprintf(link,sizeof(link),"/proc/self/fd/%d",fd);
    ssize_t length=readlink(link,path,sizeof(path)-1);
    if (length<0) length=0;
    path[length]=0;
    size_t n=0;
    for (ssize_t i=0;i<length && n+7<sizeof(escaped);i++) {
        unsigned char c=(unsigned char)path[i];
        if (c=='"' || c=='\\') { escaped[n++]='\\';escaped[n++]=c; }
        else if (c<32) { n+=(size_t)snprintf(escaped+n,sizeof(escaped)-n,"\\u%04x",c); }
        else escaped[n++]=c;
    }
    escaped[n]=0;
    int count=snprintf(line,sizeof(line),"fw_slow_io {\"op\":\"%s\",\"pid\":%ld,\"tid\":%ld,\"fd\":%d,\"path\":\"%s\",\"bytes\":%" PRIu64 ",\"offset\":%" PRId64 ",\"result\":%" PRId64 ",\"errno\":%d,\"unix_end_us\":%" PRIu64 ",\"elapsed_us\":%" PRIu64 "}\n",op,(long)getpid(),(long)syscall(SYS_gettid),fd,escaped,bytes,(int64_t)offset,result,result<0?saved_errno:0,unix_end,elapsed);
    if (count>0 && (size_t)count<sizeof(line)) (void)syscall(SYS_write,trace_fd,line,(size_t)count);
}
ssize_t pwrite(int fd,const void *buf,size_t count,off_t offset) {
    pthread_once(&initialized,initialize);uint64_t start=micros(CLOCK_MONOTONIC);
    ssize_t result=next_pwrite(fd,buf,count,offset);int e=errno;
    report("pwrite",fd,count,offset,result,e,start);errno=e;return result;
}
ssize_t pwritev(int fd,const struct iovec *iov,int count,off_t offset) {
    pthread_once(&initialized,initialize);uint64_t bytes=0;
    uint64_t start=micros(CLOCK_MONOTONIC);
    ssize_t result=next_pwritev(fd,iov,count,offset);int e=errno;
    if (result>=0) for (int i=0;i<count;i++) bytes+=iov[i].iov_len;
    report("pwritev",fd,bytes,offset,result,e,start);errno=e;return result;
}
int fsync(int fd) {
    pthread_once(&initialized,initialize);uint64_t start=micros(CLOCK_MONOTONIC);
    int result=next_fsync(fd);int e=errno;
    report("fsync",fd,0,0,result,e,start);errno=e;return result;
}
int fdatasync(int fd) {
    pthread_once(&initialized,initialize);uint64_t start=micros(CLOCK_MONOTONIC);
    int result=next_fdatasync(fd);int e=errno;
    report("fdatasync",fd,0,0,result,e,start);errno=e;return result;
}
