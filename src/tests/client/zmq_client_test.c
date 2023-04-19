// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <string.h>
#include <unistd.h>    // close()
#include <sys/types.h> // open()
#include <sys/stat.h>  // open()
#include <fcntl.h>     // open()
#include <sys/mman.h>  // mmap()
#include <sys/time.h>  // struct timeval, gettimeofday()
#include <errno.h>

#include <glib.h>
#include <glib/gprintf.h>

#include "../../client/c/zmq_interface.h"
#include "../../common/define.hh"

#define TEST_CACHE_NAME_WRITE      "/tmp/hello.write"
#define TEST_CACHE_NAME_READ       "/tmp/hello.read"
#define TEST_FILE_NAME             "HelloWorld.small"
#define TEST_FILE_NAME_2           "HelloWorld.smallsmall"
#define TEST_FILE_NAME_3           "HelloWorld.smallsmallsmall"
#define TEST_RENAME_FILE_NAME      "HelloHelloWorld.small"
#define TEST_COPY_FILE_NAME        "HelloHelloWorld.small.copy"
#define TEST_LARGE_FILE_NAME       "HelloWorld.txt"
#define TEST_FILE_LENGTH           ((unsigned long int) 128 << 20)
//#define TEST_LARGE_FILE_CACHE      "/home/ncsgroup/hwchan/snccloud/build/12G"
//#define TEST_LARGE_FILE_LENGTH     ((unsigned long int) 12884901888)
//#define TEST_LARGE_FILE_CACHE      "/home/ncsgroup/hwchan/snccloud/build/7G"
//#define TEST_LARGE_FILE_LENGTH     ((unsigned long int) 7516192768)
#define TEST_LARGE_FILE_CACHE      "/home/ncsgroup/hwchan/snccloud/build/1G"
#define TEST_LARGE_FILE_LENGTH     ((unsigned long int) 1073741824)
#define TEST_FILE_CODING           "STANDARD"
#define TEST_NUM_SPLIT_PER_GROUP   (2)
#define HUMAN_BYTE_STRING_LEN      (128)
#define TEST_NAMESPACE_ID          (-1)

unsigned char data[TEST_FILE_LENGTH];
int cached = 0;
ncloud_conn_t conn;

void convert_to_human_bytes(unsigned long int bytes, char *buf) {
    if (buf == NULL) return;
    if (bytes < ((unsigned long int) 1 << 10)) // B
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%luB", bytes);
    else if (bytes < ((unsigned long int) 1 << 20)) // KB
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%.2lfKB", bytes * 1.0 / (1 << 10));
    else if (bytes < ((unsigned long int) 1 << 30)) // MB
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%.2lfMB", bytes * 1.0 / (1 << 20));
    else if (bytes < ((unsigned long int) 1 << 40)) // GB
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%.2lfGB", bytes * 1.0 / (1 << 30));
}

double convert_to_seconds(const struct timeval t) {
    return t.tv_sec * 1.0 + t.tv_usec / 1e-6;
}

int write_test(char *name) {
    request_t req;

    set_buffered_file_write_request(&req, name, TEST_FILE_LENGTH, data, TEST_FILE_CODING, TEST_NAMESPACE_ID);

    // write file
    if (send_request(&conn, &req) == req.file.size) {
        printf("> Complete test on writing buffered file.\n");
    } else {
        printf("> Failed test on writing buffered file!\n");
        request_t_release(&req);
        return -1;
    }

    // write cached file
    FILE *f = fopen(TEST_CACHE_NAME_WRITE, "w");
    if (f == NULL) {
        fprintf(stderr, "Failed to open cache file path for write\n");
        request_t_release(&req);
        return -1;
    }
    unsigned long int filesize = 0;
    while (filesize < req.file.size) {
        size_t ret = fwrite(data + filesize, 1, TEST_FILE_LENGTH - filesize, f);
        if (ret < 0) {
            fprintf(stderr, "Failed to fill content of cache file\n");
            fclose(f);
            request_t_release(&req);
            return -1;
        }
        filesize += ret;
    }
    fclose(f);

    set_cached_file_write_request(&req, name, TEST_FILE_LENGTH, TEST_CACHE_NAME_WRITE, TEST_FILE_CODING, TEST_NAMESPACE_ID);
    if (send_request(&conn, &req) == req.file.size) {
        printf("> Complete test on writing small cached file.\n");
    } else {
        printf("> Failed test on writing small cached file!\n");
        request_t_release(&req);
        return -1;
    }

    request_t_release(&req);
    return 0;
}

int partial_overwrite_test(char *name) {
    request_t req;

    set_get_append_size_request(&req, TEST_FILE_CODING);
    if (send_request(&conn, &req) == -1) {
        printf("> Failed to get append size!\n");
        request_t_release(&req);
        return -1;
    }
    unsigned long int splitSize = req.file.length;

    //unsigned long int offset = splitSize + 20, length = splitSize;
    unsigned long int offset = splitSize / 4, length = splitSize;
    data[offset] = 's';
    data[offset+1] = 'h';
    data[offset+2] = 'b';
    set_buffered_file_overwrite_request(&req, name, data + offset, offset, length, TEST_NAMESPACE_ID);

    if (send_request(&conn, &req) == req.file.size) {
        printf("> Complete test on overwriting small buffered file.\n");
    } else {
        printf("> Failed test on overwriting small buffered file!\n");
        request_t_release(&req);
        return -1;
    }

    request_t_release(&req);
    return 0;
}

int read_test(char *name) {
    request_t req;
    set_buffered_file_read_request(&req, name, TEST_NAMESPACE_ID);
    unsigned long int ret = send_request(&conn, &req);
    // check file size
    if (TEST_FILE_LENGTH != req.file.size) {
        printf("> Failed to read file back, incorrect file size, expect %lu but got %lu!\n", TEST_FILE_LENGTH, req.file.size);
        free(req.file.data);
        request_t_release(&req);
        return -1;
    }
    if (memcmp(data, req.file.data, TEST_FILE_LENGTH) == 0) {
        printf("> Complete test on reading small buffered file.\n");
    } else {
        printf("> Failed to read file back, file is corrupted!\n");
        int fd = open("download.tmp", O_CREAT | O_WRONLY, 0644);
        if (fd != -1) {
            int ret = write(fd, req.file.data, req.file.size);
            close(fd);
        } else
            printf("> Failed to open output file\n");
        free(req.file.data);
        request_t_release(&req);
        return -1;
    }
    free(req.file.data);

    set_cached_file_read_request(&req, name, TEST_CACHE_NAME_READ, TEST_NAMESPACE_ID);   
    ret = send_request(&conn, &req);
    // check file size 
    if (TEST_FILE_LENGTH != req.file.size) {
        printf("> Failed to read file back, incorrect file size, expect %lu but got %lu!\n", TEST_FILE_LENGTH, req.file.size);
        free(req.file.data);
        request_t_release(&req);
        return -1;
    }
    // check file content
    int fd = open(req.file.cachepath.name, O_RDONLY);
    if (fd == -1) {
        printf("> Failed to open cached file for read\n");
        request_t_release(&req);
        return -1;
    }
    req.file.data = (unsigned char *) mmap (NULL, req.file.size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (req.file.data == MAP_FAILED) {
        printf("> Failed to memory map cached file for read\n");
        req.file.data = 0;
        request_t_release(&req);
        return -1;
    }
    req.file.is_data_mmap = 1;
    req.file.free_data = 1;
    if (memcmp(data, req.file.data, TEST_FILE_LENGTH) == 0) {
        printf("> Complete test on reading small cached file.\n");
    } else {
        printf("> Failed to read file back, file is corrupted!\n");
        request_t_release(&req);
        return -1;
    }

    request_t_release(&req);
    return 0;
}

int stat_test() {
    request_t req;
    set_get_storage_capacity_request(&req);
    send_request(&conn, &req);
    printf("> Get storage usage = %lu capacity = %lu; file usage count = %lu limit = %lu\n", req.stats.usage, req.stats.capacity, req.stats.file_count, req.stats.file_limit);

    set_get_file_list_request(&req, TEST_NAMESPACE_ID, "");
    if (send_request(&conn, &req) == -1) {
        printf("> Request failed\n");
    } else {
        printf("Get a total of %u files\n", req.file_list.total);
        for (unsigned int i = 0; i < req.file_list.total; i++) {
            printf("Get file [%s] of size %lu\n\tcreate at %s",
                req.file_list.list[i].fname,
                req.file_list.list[i].fsize,
                ctime(&req.file_list.list[i].ctime)
            );
            printf("\tlast access at %s",
                ctime(&req.file_list.list[i].atime)
            );
            printf("\tlast modified at %s",
                ctime(&req.file_list.list[i].mtime)
            );
        }
    }
    request_t_release(&req);

    set_get_repair_stats_request(&req);
    send_request(&conn, &req);
    printf("> Get repair stats: %lu of %lu files %s pending for repair / under repair\n", req.stats.file_limit, req.stats.file_count, req.stats.file_limit > 1? "are" : "is");
    request_t_release(&req);

    return 0;
}

int sys_status_test() {
    request_t req;
    set_get_agent_status_request(&req);
    send_request(&conn, &req);
    printf("> Get agent status, num of agents = %u\n", req.agent_list.total);
    char usage[HUMAN_BYTE_STRING_LEN], capacity[HUMAN_BYTE_STRING_LEN];

    for (unsigned int i = 0; i < req.agent_list.total; i++) {
        printf("  Agent %d status = %s, num of containers = %d\n", i + 1, req.agent_list.list[i].alive? "alive" : "offline", req.agent_list.list[i].num_containers);
        for (int j = 0; j < req.agent_list.list[i].num_containers; j++) {
            convert_to_human_bytes(req.agent_list.list[i].container_usage[j], usage);
            convert_to_human_bytes(req.agent_list.list[i].container_capacity[j], capacity);
            printf("    container id = %d, usage = %lu (%s), capacity = %lu (%s)\n",
                    req.agent_list.list[i].container_id[j],
                    req.agent_list.list[i].container_usage[j],
                    usage,
                    req.agent_list.list[i].container_capacity[j],
                    capacity
            );
        }
    }

    request_t_release(&req);
    return 0;
}

int delete_test(char *name) {
    int ret = 0;
    request_t req;
    set_delete_file_request(&req, name, TEST_NAMESPACE_ID);
    ret = send_request(&conn, &req);
    if (ret == -1) {
        printf("> Request to delete %s failed\n", name);
    }
    return ret;
}

int large_file_delete() {
    request_t req;
    // delete 
    set_delete_file_request(&req, TEST_LARGE_FILE_NAME, TEST_NAMESPACE_ID);
    if (send_request(&conn, &req) == -1) {
        printf("> Request to delete %s failed\n", TEST_LARGE_FILE_NAME);
        request_t_release(&req);
        return -1;
    }

    request_t_release(&req);
    return 0;
}

int large_file_test() {
    request_t req;
    struct timeval start, end;
    /*
    // write large cached file
    set_cached_file_write_request(&req, TEST_LARGE_FILE_NAME, TEST_LARGE_FILE_LENGTH, TEST_LARGE_FILE_CACHE, TEST_FILE_CODING, TEST_NAMESPACE_ID);
    if (send_request(&conn, &req) == req.file.size) {
        printf("> Complete test on writing large cached file.\n");
    } else {
        printf("> Failed test on writing large cached file!\n");
        request_t_release(&req);
        return -1;
    }

    // read and delete
    request_t req;
    // read large cached file
    set_cached_file_read_request(&req, TEST_LARGE_FILE_NAME, TEST_CACHE_NAME_READ, TEST_NAMESPACE_ID);
    if (send_request(&conn, &req) == TEST_LARGE_FILE_LENGTH) {
        printf("> Complete test on reading large cached file.\n");
    } else {
        printf("> Failed test on reading large cached file!\n");
        request_t_release(&req);
        return -1;
    }

    if (large_file_delete() == -1)
        return -1;

    */

    gettimeofday(&start, NULL);

    // get append size
    set_get_append_size_request(&req, TEST_FILE_CODING);
    if (send_request(&conn, &req) == -1) {
        printf("> Failed to get append size!\n");
        request_t_release(&req);
        return -1;
    }
    unsigned long int splitSize = req.file.length;
    unsigned char *data;

    if (cached) {
        // init the transfer
        set_cached_file_write_request(&req, TEST_LARGE_FILE_NAME, splitSize, TEST_LARGE_FILE_CACHE, TEST_FILE_CODING, TEST_NAMESPACE_ID);
        if (send_request(&conn, &req) != req.file.size) {
            printf("> Failed test on writing large cached file!\n");
            request_t_release(&req);
            return -1;
        }
    } else {
        int fd = open(TEST_LARGE_FILE_CACHE, O_RDONLY);
        if (fd == -1) {
            request_t_release(&req);
            return -1;
        }
        data = (unsigned char *) mmap (NULL, TEST_LARGE_FILE_LENGTH, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (data == NULL) {
            request_t_release(&req);
            return -1;
        }
        set_buffered_file_write_request(&req, TEST_LARGE_FILE_NAME, splitSize, data, TEST_FILE_CODING, TEST_NAMESPACE_ID);
        if (send_request(&conn, &req) != req.file.size) {
            printf("> Failed test on writing large cached file!\n");
            request_t_release(&req);
            return -1;
        }
    }
    // continue to append
    int splitCount = TEST_LARGE_FILE_LENGTH / splitSize + (TEST_LARGE_FILE_LENGTH % splitSize == 0? 0 : 1);
    for (int i = 1; i < splitCount; i += TEST_NUM_SPLIT_PER_GROUP) {
        unsigned long int offset = i * splitSize; 
        unsigned long int remains = TEST_LARGE_FILE_LENGTH - offset;
        unsigned long int length = (remains > splitSize * TEST_NUM_SPLIT_PER_GROUP ? splitSize * TEST_NUM_SPLIT_PER_GROUP: remains);
        if (cached) {
            set_cached_file_append_request(&req, TEST_LARGE_FILE_NAME, TEST_LARGE_FILE_CACHE, offset, length, TEST_NAMESPACE_ID);
        } else {
            set_buffered_file_append_request(&req, TEST_LARGE_FILE_NAME, data + offset, offset, length, TEST_NAMESPACE_ID);
        }
        unsigned long int fsize = send_request(&conn, &req) ;
        if (fsize == -1) {
            printf("> Failed test on appending large cached file!\n");
            request_t_release(&req);
            return -1;
        }
        if (fsize != length + offset) {
            printf("> Failed test on appending large cached file! expect file size %lu, but got %lu\n", length + offset, fsize);
            request_t_release(&req);
            return -1;
        }
    }
    gettimeofday(&end, NULL);
    double st = convert_to_seconds(start), et = convert_to_seconds(end);
    char fsizestr[HUMAN_BYTE_STRING_LEN];
    convert_to_human_bytes(TEST_LARGE_FILE_LENGTH, fsizestr);
    //printf("> Complete test on writing large%s file, speed = %.3lfMB/s (%s in %.6lf seconds)\n", cached? " cache" : "", TEST_LARGE_FILE_LENGTH / (et - st), fsizestr, et - st);
    printf("> Complete test on write large%s file\n", cached? " cache" : "");

    if (!cached) {
        munmap(data, TEST_LARGE_FILE_LENGTH);
    }

    gettimeofday(&start, NULL);
    // read
    set_get_read_size_request(&req, TEST_LARGE_FILE_NAME, TEST_NAMESPACE_ID);
    if (send_request(&conn, &req) == 0) {
        printf("> Failed to get read size\n");
        request_t_release(&req);
        return -1;
    }

    int fd = 0;
    if (!cached) {
        fd = open(TEST_CACHE_NAME_READ, O_RDWR);
        if (fd == -1) {
            request_t_release(&req);
            return -1;
        }
        if (ftruncate(fd, TEST_LARGE_FILE_LENGTH) != 0)
            printf("> Failed to truncate file to destinated length %lu\n", TEST_LARGE_FILE_LENGTH);
    }

    long pageSize = sysconf(_SC_PAGE_SIZE);
    splitSize = req.file.length;
    splitCount = TEST_LARGE_FILE_LENGTH / splitSize + (TEST_LARGE_FILE_LENGTH % splitSize == 0? 0 : 1);
    for (int i = 0; i < splitCount; i += TEST_NUM_SPLIT_PER_GROUP) {
        unsigned long int offset = i * splitSize; 
        unsigned long int remains = TEST_LARGE_FILE_LENGTH - offset;
        unsigned long int length = splitSize * TEST_NUM_SPLIT_PER_GROUP;
            //(remains > splitSize * TEST_NUM_SPLIT_PER_GROUP ? splitSize * TEST_NUM_SPLIT_PER_GROUP: remains);
        unsigned long int aoffset = offset / pageSize * pageSize;
        unsigned long int alength = (offset + length - aoffset + pageSize - 1) / pageSize * pageSize;
        if (cached) {
            set_cached_file_partial_read_request(&req, TEST_LARGE_FILE_NAME, TEST_CACHE_NAME_READ, offset, length, TEST_NAMESPACE_ID);
        } else {
            data = (unsigned char *) mmap (NULL, alength, PROT_WRITE, MAP_SHARED, fd, aoffset);
            if (data == MAP_FAILED) {
                printf("> Failed to mmap for outputing read data on range %ld-%ld, %s\n", aoffset, alength, strerror(errno));
                continue;
            }
            set_buffered_file_partial_read_request(&req, TEST_LARGE_FILE_NAME, data + (offset - aoffset), offset, length, TEST_NAMESPACE_ID);
        }
        unsigned long int rsize = send_request(&conn, &req);
        if (rsize == -1 || req.file.offset != offset) {
            printf("> Failed test on reading large cached file, ret = %ld offset = %ld!\n", rsize, req.file.offset);
            request_t_release(&req);
            return -1;
        }
        if (!cached) {
            munmap(data, alength);
        }
        printf(">> Read offset = %lu size = %lu\n", offset, rsize);
    }
    gettimeofday(&end, NULL);
    st = convert_to_seconds(start);
    et = convert_to_seconds(end);
    //printf("> Complete test on reading large%s file, speed = %.3lfMB/s (%s in %.6lf seconds)\n", cached? " cache" : "", TEST_LARGE_FILE_LENGTH / (et - st), fsizestr, et - st);
    printf("> Complete test on reading large%s file\n", cached? " cache" : "");

    if (!cached) {
        close(fd);
    }

    // delete
    if (large_file_delete() == -1)
        return -1;

    request_t_release(&req);
    return 0;
}

int rename_test(char *sname, char *dname) {
    request_t req;

    // rename
    set_file_rename_request(&req, sname, dname, TEST_NAMESPACE_ID);
    if (send_request(&conn, &req) != -1) {
        printf("> Complete tests on renaming file\n");
    } else {
        printf("> Failed tests on renaming file\n");
        request_t_release(&req);
        return -1;
    }

    request_t_release(&req);
    return 0;
}

int copy_test(char *sname, char *dname) {
    request_t req;

    set_get_read_size_request(&req, sname, TEST_NAMESPACE_ID);
    
    if (send_request(&conn, &req) == -1) {
        printf("> Failed get read size for copying file\n");
        request_t_release(&req);
        return -1;
    }

    unsigned long int rsize = req.file.length, copied = 0;
    request_t_release(&req);

    // copy 2 stripe at a time
    for (copied = 0; copied < TEST_FILE_LENGTH; copied += rsize * 2) {
        if (set_file_copy_request(&req, sname, dname, copied, rsize * 2, TEST_NAMESPACE_ID) == -1 || send_request(&conn, &req) == -1) {
            printf("> Failed tests on copying file\n");
            request_t_release(&req);
            return -1;
        }
        request_t_release(&req);
    }

    printf("> Complete tests on copying file\n");
    return 0;
}

int main() {
    // init file
    for (int i = 0; i < TEST_FILE_LENGTH; i++)
        data[i] = (unsigned char) i % 256;

    g_autoptr(GError) error = 0; 
    g_autoptr(GKeyFile) proxy_config = g_key_file_new();
    g_autoptr(GKeyFile) general_config = g_key_file_new();
    
    if (!g_key_file_load_from_file(proxy_config, "proxy.ini", G_KEY_FILE_NONE, &error)) {
        fprintf(stderr, "Failed to load proxy.ini, %s\n", error->message);
        return -1;
    }

    if (!g_key_file_load_from_file(general_config, "general.ini", G_KEY_FILE_NONE, &error)) {
        fprintf(stderr, "Failed to load general.ini, %s\n", error->message);
        return -1;
    }

    // get the proxy information for connecting zero-mq interface
    gint proxy_num = g_key_file_get_integer(proxy_config, "proxy", "num", &error);
    char proxy_key[32];
    snprintf(proxy_key, 32, "proxy%02d", proxy_num);
    error = 0;
    const gchar *ip = g_key_file_get_string(general_config, proxy_key, "ip", &error);
    if (error)
        ip = "127.0.0.1";
    error = 0;
    gint port = g_key_file_get_integer(proxy_config, "zmq_interface", "port", &error);
    if (error)
        port = 59001;

    // init connection
    ncloud_conn_t_init(ip, port, &conn, 1);

    // write 2 files
    if (write_test(TEST_FILE_NAME) == -1) {
        fprintf(stderr, "Write test FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    if (write_test(TEST_FILE_NAME_2) == -1) {
        fprintf(stderr, "Write test (2nd time) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // read both files
    if (read_test(TEST_FILE_NAME) == -1) {
        fprintf(stderr, "Read test FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    if (read_test(TEST_FILE_NAME_2) == -1) {
        fprintf(stderr, "Read test (2nd time) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    printf("Wait for background tasks to complete\n");
    sleep(10);
    // rename to an existing file
    if (rename_test(TEST_FILE_NAME, TEST_FILE_NAME_2) == -1) {
        fprintf(stderr, "Rename test (1st) failed while expecting a success!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // rename to non-existing file
    if (rename_test(TEST_FILE_NAME_2, TEST_RENAME_FILE_NAME) == -1) {
        fprintf(stderr, "Rename test (2nd) failed while expecting a success!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // rename a non-existing file
    if (rename_test(TEST_FILE_NAME_2, TEST_RENAME_FILE_NAME) != -1) {
        fprintf(stderr, "Rename test (3rd) succeeded while expecting a failure!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // rename a non-existing file to a non-existing file
    if (rename_test(TEST_FILE_NAME_2, TEST_FILE_NAME_3) != -1) {
        fprintf(stderr, "Rename test (3rd) succeeded while expecting a failure!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // read renamed file
    if (read_test(TEST_RENAME_FILE_NAME) == -1) {
        fprintf(stderr, "Read test (renamed file) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // overwrite
    if (write_test(TEST_RENAME_FILE_NAME) == -1) {
        fprintf(stderr, "Write test (2nd time) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // read again
    if (read_test(TEST_RENAME_FILE_NAME) == -1) {
        fprintf(stderr, "Read test (renamed file) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // partial overwrite
    if (partial_overwrite_test(TEST_RENAME_FILE_NAME)) {
        fprintf(stderr, "Partial Overwrite test FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // read again
    if (read_test(TEST_RENAME_FILE_NAME) == -1) {
        fprintf(stderr, "Read test (renamed file) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // copy a file
    if (copy_test(TEST_RENAME_FILE_NAME, TEST_COPY_FILE_NAME) == -1) {
        fprintf(stderr, "Copy file test FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // copy a file to itself (locked)
    if (copy_test(TEST_RENAME_FILE_NAME, TEST_RENAME_FILE_NAME) != -1) {
        fprintf(stderr, "Copy file test (on locked file) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // read copied file
    if (read_test(TEST_COPY_FILE_NAME) == -1) {
        fprintf(stderr, "Read test (copied file) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    // delete all files created
    if (delete_test(TEST_RENAME_FILE_NAME) == -1) {
        fprintf(stderr, "Delete test (renamed file) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    if (delete_test(TEST_COPY_FILE_NAME) == -1) {
        fprintf(stderr, "Delete test (copied file) FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    /*
    if (large_file_test() == -1) {
        fprintf(stderr, "Large file test FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    */
    if (stat_test() == -1) {
        fprintf(stderr, "Stat test FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }
    if (sys_status_test() == -1) {
        fprintf(stderr, "System status test FAILED!\n");
        ncloud_conn_t_release(&conn);
        return -1;
    }

    // close connection
    ncloud_conn_t_release(&conn);

    return 0;
}
