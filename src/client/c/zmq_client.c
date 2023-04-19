// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <errno.h> // errno
#include <string.h> // strerror
#include <limits.h> // ULONG_MAX

#include "../../client/c/zmq_interface.h"

#define NUM_REQUIRED_ARG (2)

const unsigned char namespaceId = 1;

void usage(const char *prog) {
    printf("Usage: %s <file1> [file2] ... \n", prog);
}

int writeObject(const char *ip, int port, char *class, FILE *f, char *name) {
    request_t req;
    ncloud_conn_t conn;
    ncloud_conn_t_init(ip, port, &conn, 1);

    int ret = 0, bytes = 0;
    unsigned long int splitSize = 0, bytesRead = 0, written = 0;
    unsigned char *data = NULL;

    // get append size
    set_get_append_size_request(&req, class);
    if (send_request(&conn, &req) == -1) {
        fprintf(stderr, "> Failed to get append size!\n");
        request_t_release(&req);
        ret = -1;
        goto write_object_exit;
    }

    splitSize = req.file.length;
    request_t_release(&req);

    data = (unsigned char *) malloc(splitSize);

    while (!feof(f)) {
        // read data for write
        bytes = fread(data, 1, splitSize, f);
        if (bytes < 0) {
            fprintf(stderr, "Failed to read data, %s\n", strerror(errno));
            break;
        }
        
        // set the request: write for the first split, append for others
        if (bytesRead == 0) {
            set_buffered_file_write_request(&req, name, bytes, data, class, namespaceId);
        } else {
            unsigned long int offset = bytesRead, length = bytes;
            set_buffered_file_append_request(&req, name, data, offset, length, namespaceId);
        }

        // write split 
        written = send_request(&conn, &req);
        if ((bytesRead == 0 && written == req.file.size) || (bytesRead > 0 && written == req.file.offset + bytes)) {
        } else {
            printf("> Failed to write file %s at offset %lu, returned size = %lu, length = %lu, offset = %lu\n", name, bytesRead, req.file.size, req.file.length, req.file.offset);
            request_t_release(&req);
            goto write_object_exit;
        }

        bytesRead += bytes;

        request_t_release(&req);
    }

    printf("> Write file %s.\n", name);

write_object_exit:
    free(data);
    ncloud_conn_t_release(&conn);

    return ret;
}

int readObject(const char *ip, int port, FILE *f, char *name) {
    request_t req;
    ncloud_conn_t conn;
    ncloud_conn_t_init(ip, port, &conn, 1);

    int ret = 0;
    unsigned long int splitSize = 0, bytesRead = 0;
    unsigned long int lengthReadFromCloud = 0, lengthReadFromLocal = 0;
    unsigned char *dataLocal = NULL, *dataFromCloud = NULL;

    // get the expected read size
    set_get_read_size_request(&req, name, namespaceId);
    if (send_request(&conn, &req) == 0) {
        printf("> Failed to get read size\n");
        request_t_release(&req);
        ret = -1;
        goto read_object_exit;
    }

    splitSize = req.file.length;
    request_t_release(&req);

    dataFromCloud = (unsigned char *) malloc(splitSize);
    dataLocal = (unsigned char *) malloc(splitSize);

    // read data back and compare
    while (!feof(f)) {
        unsigned long int offset = bytesRead;
        // read from ncloud
        set_buffered_file_partial_read_request(&req, name, dataFromCloud, offset, splitSize, namespaceId);
        lengthReadFromCloud = send_request(&conn, &req);
        if (lengthReadFromCloud < 0) {
            fprintf(stderr, "Failed to read file %s at offset %lu\n", name, offset);
            ret = -1;
            goto read_object_exit;
        }
        // check data length
        lengthReadFromLocal = fread(dataLocal, 1, splitSize, f);
        if (lengthReadFromLocal != lengthReadFromCloud) {
            fprintf(stderr, "File %s length unmatched at offset %lu (expected %lu but got %lu)\n", name, offset, lengthReadFromLocal, lengthReadFromCloud);
            ret = -1;
            goto read_object_exit;
        }
        // check data content
        if (memcmp(dataLocal, dataFromCloud, lengthReadFromLocal) != 0) {
            fprintf(stderr, "File %s data corrupted at %lu,%lu\n", name, offset, lengthReadFromLocal);
            ret = -1;
            goto read_object_exit;
        }

        bytesRead += lengthReadFromLocal;

        request_t_release(&req);
    }

    // final check on file size
    set_buffered_file_partial_read_request(&req, name, dataFromCloud, bytesRead, splitSize, namespaceId);
    lengthReadFromCloud = send_request(&conn, &req);
    if (lengthReadFromCloud != ULONG_MAX && lengthReadFromCloud != 0) {
        fprintf(stderr, "File length mismatched for file %s, file on cloud is larger than the local one (%lu)\n", name, lengthReadFromCloud);
        ret = -1;
        goto read_object_exit;
    }

    printf("> Read file %s.\n", name);

read_object_exit:
    free(dataLocal);
    free(dataFromCloud);
    return ret;

}

int renameObject(const char *ip, int port, char *oldName, char *newName) {
    request_t req;
    ncloud_conn_t conn;
    ncloud_conn_t_init(ip, port, &conn, 1);

    int ret = 0;

    set_file_rename_request(&req, oldName, newName, namespaceId);
    if (send_request(&conn, &req) == -1) {
        printf("> Failed to rename file %s\n", oldName);
        ret = -1;
    } else {
        printf("> Rename file %s to %s.\n", oldName, newName);
    }

    request_t_release(&req);
    ncloud_conn_t_release(&conn);

    return ret;
}

int deleteObject(const char *ip, int port, char *name) {
    request_t req;
    ncloud_conn_t conn;
    ncloud_conn_t_init(ip, port, &conn, 1);

    int ret = 0;
    set_delete_file_request(&req, name, namespaceId);

    if (send_request(&conn, &req) == -1) {
        printf("> Failed to delete file %s\n", name);
        ret = -1;
    } else {
        printf("> Delete file %s.\n", name);
    }

    request_t_release(&req);
    return 0;
}

int getStorageUsage(const char *ip, int port) {
    request_t req;
    ncloud_conn_t conn;
    ncloud_conn_t_init(ip, port, &conn, 1);

    set_get_storage_capacity_request(&req);
    send_request(&conn, &req);
    printf("> Get storage usage = %lu capacity = %lu; file usage count = %lu limit = %lu\n", req.stats.usage, req.stats.capacity, req.stats.file_count, req.stats.file_limit);

    request_t_release(&req);
    ncloud_conn_t_release(&conn);

    return 0;
}

int listObjects(const char *ip, int port) {
    request_t req;
    ncloud_conn_t conn;
    ncloud_conn_t_init(ip, port, &conn, 1);

    set_get_file_list_request(&req, namespaceId, "");
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
    ncloud_conn_t_release(&conn);

    return 0;
}

void storageStatus(const char *ip, int port) {
    listObjects(ip, port);
    getStorageUsage(ip, port);
}

void getModifiedFilename(char *newFilename, const char *oldFilename) {
    snprintf(newFilename, 4096, "%s_new", oldFilename);
}

int main(int argc, char **argv) {
    const char *ip = "127.0.0.1";
    int port = 59001;
    char *class = "STANDARD";

    if (argc < NUM_REQUIRED_ARG) {
        usage(argv[0]);
        return -1;
    }

    storageStatus(ip, port);

    char newFilename[4096];
    for (int i = 1; i < argc; i++) {
        char *filename = argv[i];
        FILE *f = fopen(filename, "rb");

        // open source file
        if (f == NULL) {
            fprintf(stderr, "> Cannot open file %s, %s\n", filename, strerror(errno));
            continue;
        }

        // write file to ncloud
        if (writeObject(ip, port, class, f, filename) != 0) {
            fprintf(stderr, "> Cannot write file %s\n", filename);
            fclose(f);
            continue;
        }

        // read file from ncloud (and compare)
        fseek(f, 0, SEEK_SET);
        if (readObject(ip, port, f, filename) != 0) {
            fprintf(stderr, "> Cannot read file %s\n", filename);
            fclose(f);
            continue;
        }

        // rename file in ncloud
        getModifiedFilename(newFilename, filename);
        if (renameObject(ip, port, filename, newFilename) != 0) {
            fprintf(stderr, "> Cannot rename file %s to %s\n", filename, newFilename);
        }

        fclose(f);
    }

    storageStatus(ip, port);

    for (int i = 1; i < argc; i++) {
        char *filename = argv[i];
        // delete files from ncloud
        deleteObject(ip, port, filename);
        getModifiedFilename(newFilename, filename);
        deleteObject(ip, port, newFilename);
    }

    storageStatus(ip, port);

    return 0;
}
