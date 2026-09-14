#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>
int main(void) {
 char path[]="/tmp/fireweed-io-smoke-XXXXXX";int fd=mkstemp(path);assert(fd>=0);
 assert(pwrite(fd,"abcd",4,0)==4);
 struct iovec v[2]={{"ef",2},{"gh",2}};assert(pwritev(fd,v,2,4)==4);
 assert(fsync(fd)==0);assert(fdatasync(fd)==0);
 char b[8];assert(pread(fd,b,8,0)==8);assert(memcmp(b,"abcdefgh",8)==0);
 errno=0;assert(pwrite(-1,"x",1,0)==-1 && errno==EBADF);
 errno=0;assert(pwritev(-1,v,2,0)==-1 && errno==EBADF);
 assert(close(fd)==0);assert(unlink(path)==0);return 0;
}
