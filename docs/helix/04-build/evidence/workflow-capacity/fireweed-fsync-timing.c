#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
static int (*original_fsync)(int);
static int (*original_fdatasync)(int);
__attribute__((constructor)) static void init(void) {
    original_fsync = dlsym(RTLD_NEXT, "fsync");
    original_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
}
static int timed_sync(int fd, int data_only) {
    char link[64], path[4096], output[256];
    struct stat st;
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, sizeof(path)-1);
    int measured = n >= 0;
    if (measured) { path[n] = 0; measured = strstr(path, "/log/") != NULL; }
    int directory = measured && fstat(fd, &st) == 0 && S_ISDIR(st.st_mode);
    const char *class = measured && strstr(path, "/fwlog") ? "segment" :
                        measured && strstr(path, "/manifest") ? "manifest" : "metadata";
    struct timespec before, after;
    clock_gettime(CLOCK_MONOTONIC, &before);
    int result = data_only ? original_fdatasync(fd) : original_fsync(fd);
    int saved_errno = errno;
    clock_gettime(CLOCK_MONOTONIC, &after);
    if (measured) {
        int64_t ns = (int64_t)(after.tv_sec-before.tv_sec)*1000000000 + after.tv_nsec-before.tv_nsec;
        int count = snprintf(output, sizeof(output), "log_sync class=%s kind=%s call=%s us=%" PRId64 " result=%d\n",
            class, directory ? "directory" : "file", data_only ? "fdatasync" : "fsync", ns/1000, result);
        if (count > 0 && count < (int)sizeof(output)) (void)write(STDERR_FILENO, output, count);
    }
    errno = saved_errno;
    return result;
}
int fsync(int fd) { return timed_sync(fd, 0); }
int fdatasync(int fd) { return timed_sync(fd, 1); }
