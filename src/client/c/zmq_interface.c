// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <string.h>    // strlen()
#include <stdlib.h>    // memcpy()
#include <unistd.h>    // close()
#include <sys/types.h> // open()
#include <sys/stat.h>  // open()
#include <fcntl.h>     // open()
#include <sys/mman.h>  // mmap()
#include <time.h>      // time()
#include <limits.h>    // ULONG_MAX

#include <zmq.h>

#include "zmq_interface.h"
#include "../../common/define.hh"

#define RECV_TIMEOUT     (300 * 1000) // ms

#ifdef NDEBUG
    #define log_info(...)    ;
    #define log_msg(...)     ;
#else
    #define log_info(...)    fprintf(stderr, __VA_ARGS__)
    #define log_msg(...)     fprintf(stdout, __VA_ARGS__)
#endif

#define log_error(...)       fprintf(stderr, __VA_ARGS__)

static int issue_request(void *socket, int opcode, unsigned char namespace_id, file_t *file, sys_stats_t *stats, file_list_head_t *flist, agent_info_head_t *alist, sysinfo_t *pstatus);
static int has_file_data(int opcode);

static int setup_connection(const char *ip, unsigned short port, void **context, void **socket);
static int teardown_connection(void *context, void *socket);

static int set_file_write_request_base(request_t *req, char *filename, unsigned long int filesize, char *storage_class, unsigned char namespace_id);

static int has_file_data(int opcode);
static int has_opcode_only(int opcode);
static int has_namespace_id_only(int opcode);
static int has_size_or_length(int opcode);
static int has_offset(int opcode);


////////////////////////
//  Data struct init  //
////////////////////////

int name_t_init(name_t *name) {
    if (name == NULL)
        return -1;
    name->name = 0;
    name->length = 0;
    return 0;
}

void name_t_release(name_t *name) {
    if (name == NULL)
        return;
    name_t_init(name);
}

int file_t_init(file_t *file) {
    if (file == NULL)
        return -1;

    name_t_init(&file->filename);
    name_t_init(&file->cachepath);
    file->offset = 0;
    file->size = 0;
    name_t_init(&file->storage_class);
    file->data = 0;

    file->free_cachepath = 0;
    file->free_filename = 0;
    file->free_data = 0;
    file->is_data_mmap = 0;
    return 0;
}

void file_t_release(file_t *file) {
    if (file == NULL)
        return;

    if (file->free_data) {
        if (file->is_data_mmap) {
            munmap(file->data, file->size);
        } else {
            free(file->data);
        }
    }

    if (file->free_filename)
        free(file->filename.name);
    name_t_release(&file->filename);

    if (file->free_cachepath)
        free(file->cachepath.name);
    name_t_release(&file->cachepath);

    file_t_init(file);
}

int file_list_item_t_init(file_list_item_t *fitem) {
    if (fitem == NULL)
        return -1;

    fitem->fname = 0;
    fitem->fsize = 0;
    fitem->ctime = 0;
    fitem->atime = 0;
    fitem->mtime = 0;
    return 0;
}

void file_list_item_t_release(file_list_item_t *fitem) {
    if (fitem == NULL)
        return;

    free(fitem->fname);

    file_list_item_t_init(fitem);
}

int file_list_head_t_init(file_list_head_t *head) {
    if (head == NULL)
        return -1;

    head->list = 0;
    head->total = 0;
    return 0;
}

void file_list_head_t_release(file_list_head_t *head) {
    if (head == NULL)
        return;

    for (unsigned int i = 0; i < head->total; i++)
        file_list_item_t_release(&head->list[i]);
    free(head->list);

    file_list_head_t_init(head);
}

int sysinfo_t_init(sysinfo_t *sysinfo) {
    if (sysinfo == NULL)
        return -1;

    sysinfo->cpu.num = 0;
    memset(sysinfo->cpu.usage, 0, 256 * sizeof(float));
    sysinfo->mem.total = 0;
    sysinfo->mem.free = 0;
    sysinfo->net.in = 0.0;
    sysinfo->net.out = 0.0;
}

void sysinfo_t_release(sysinfo_t *sysinfo) {
    sysinfo_t_init(sysinfo);
}

int agent_info_t_init(agent_info_t *alist) {
    if (alist == NULL)
        return -1;

    alist->alive = 0;
    alist->addr = 0;
    alist->num_containers = 0;

    memset(alist->container_id, 0, sizeof(int) * MAX_NUM_CONTAINER_PER_AGENT);
    memset(alist->container_usage, 0, sizeof(unsigned long int) * MAX_NUM_CONTAINER_PER_AGENT);
    memset(alist->container_capacity, 0, sizeof(unsigned long int) * MAX_NUM_CONTAINER_PER_AGENT);

    sysinfo_t_init(&alist->sysinfo);

    return 0;
}

void agent_info_t_release(agent_info_t *alist) {
    if (alist == NULL)
        return;

    free(alist->addr);
    sysinfo_t_release(&alist->sysinfo);

    agent_info_t_init(alist);
}

int agent_info_head_t_init(agent_info_head_t *head) {
    if (head == NULL)
        return -1;

    head->list = 0;
    head->total = 0;
    return 0;
}

void agent_info_head_t_release(agent_info_head_t *head) {
    if (head == NULL)
        return;

    for (unsigned int i = 0; i < head->total; i++)
        agent_info_t_release(&head->list[i]);
    free(head->list);

    agent_info_head_t_init(head);
}

int sys_stats_t_init(sys_stats_t *stats) {
    if (stats == NULL)
        return -1;

    stats->usage = 0;
    stats->capacity = 0;
    return 0;
}

void sys_stats_t_release(sys_stats_t *stats) {
    sys_stats_t_init(stats);
}

int request_t_init(request_t *request) {
    if (request == NULL)
        return -1;

    file_t_init(&request->file);
    sys_stats_t_init(&request->stats);
    file_list_head_t_init(&request->file_list);
    agent_info_head_t_init(&request->agent_list);
    sysinfo_t_init(&request->proxy_status);

    request->opcode = UNKNOWN_CLIENT_OP;
    request->opcode = UNKNOWN_NAMESPACE_ID;

    return 0;
}

void request_t_release(request_t *request) {
    if (request == NULL)
        return;

    file_t_release(&request->file);
    sys_stats_t_release(&request->stats);
    file_list_head_t_release(&request->file_list);
    agent_info_head_t_release(&request->agent_list);
    sysinfo_t_release(&request->proxy_status);

    request_t_init(request);
}

int ncloud_conn_t_init(const char *ip, unsigned short port, ncloud_conn_t *conn, int connect) {
    if (conn == NULL)
        return -1;

    conn->socket = 0;
    conn->context = 0;

    if (connect)
        setup_connection(ip, port, &conn->context, &conn->socket);

    return 0;
}

void ncloud_conn_t_release(ncloud_conn_t *conn) {
    if (conn == NULL)
        return;

    if (conn->socket != 0 && conn->context != 0)
        teardown_connection(conn->context, conn->socket);

    ncloud_conn_t_init("", 0, conn, 0);
}


////////////////////
//  Request init  //
////////////////////

static int _set_file_request(request_t *req, char *filename, unsigned char namespace_id) {
    // file name
    req->file.filename.name = filename;
    req->file.filename.length = strlen(filename);
    req->namespace_id = namespace_id;
}

static int _set_file_ranged_request(request_t *req, char *filename, unsigned char namespace_id, unsigned long int offset, unsigned long int length) {
    // file name
    _set_file_request(req, filename, namespace_id);
    // range
    req->file.offset = offset;
    req->file.length = length;
}

static int _set_cached_file_request(request_t *req, char *filename, char *cachepath, unsigned char namespace_id) {
    // file name
    _set_file_request(req, filename, namespace_id);
    // cache name
    req->file.cachepath.name = cachepath;
    req->file.cachepath.length = strlen(cachepath);
}

static int _set_cached_file_ranged_request(request_t *req, char *filename, char *cachepath, unsigned long int offset, unsigned long int length, unsigned char namespace_id) {
    // file name and cache name
    _set_cached_file_request(req, filename, cachepath, namespace_id);
    // range
    req->file.offset = offset;
    req->file.length = length;
}

static int set_file_write_request_base(request_t *req, char *filename, unsigned long int filesize, char *storage_class, unsigned char namespace_id) {
    if (request_t_init(req) != 0 || filename == NULL || storage_class == NULL)
        return -1;
    // name
    _set_file_request(req, filename, namespace_id);
    // size
    req->file.size = filesize;
    // storage class 
    req->file.storage_class.name = storage_class;
    req->file.storage_class.length = strlen(storage_class);
    // opcode
    req->opcode = WRITE_FILE_REQ;

    return 0;
}

int set_buffered_file_write_request(request_t *req, char *filename, unsigned long int filesize, unsigned char *data, char *storage_class, unsigned char namespace_id) {
    if (data == NULL || set_file_write_request_base(req, filename, filesize, storage_class, namespace_id) == -1)
        return -1;

    // data
    req->file.data = data;

    return 0;
}

int set_cached_file_write_request(request_t *req, char *filename, unsigned long int filesize, char *cachepath, char *storage_class, unsigned char namespace_id) {
    if (cachepath == NULL || set_file_write_request_base(req, filename, filesize, storage_class, namespace_id) == -1)
        return -1;

    // cache path
    req->file.cachepath.name = cachepath;
    req->file.cachepath.length = strlen(cachepath);

    return 0;
}

int set_buffered_file_read_request(request_t *req, char *filename, unsigned char namespace_id) {
    if (request_t_init(req) != 0 || filename == NULL)
        return -1;

    // name
    _set_file_request(req, filename, namespace_id);
    req->opcode = READ_FILE_REQ;

    return 0;
}

int set_cached_file_read_request(request_t *req, char *filename, char *cachepath, unsigned char namespace_id) {
    if (request_t_init(req) != 0 || filename == NULL || cachepath == NULL)
        return -1;

    // name and cache path
    _set_cached_file_request(req, filename, cachepath, namespace_id);
    req->opcode = READ_FILE_REQ;

    return 0;
}

int set_delete_file_request(request_t *req, char* filename, unsigned char namespace_id) {
    if (request_t_init(req) != 0 || filename == NULL)
        return -1;

    // name
    _set_file_request(req, filename, namespace_id);
    req->opcode = DEL_FILE_REQ;

    return 0;
}

int set_buffered_file_append_request(request_t *req, char *filename, unsigned char *data, unsigned long int offset, unsigned long int length, unsigned char namespace_id) {
    if (request_t_init(req) != 0)
        return -1; 

    // name, range
    _set_file_ranged_request(req, filename, namespace_id, offset, length);
    // data
    req->file.data = data;
    req->opcode = APPEND_FILE_REQ;

    return 0;
}

int set_cached_file_append_request(request_t *req, char *filename, char *cachepath, unsigned long int offset, unsigned long int length, unsigned char namespace_id) {
    if (request_t_init(req) != 0)
        return -1;

    // name, cache path, range
    _set_cached_file_ranged_request(req, filename, cachepath, offset, length, namespace_id);
    req->opcode = APPEND_FILE_REQ;

    return 0;
}

int set_buffered_file_overwrite_request(request_t *req, char *filename, unsigned char *data, unsigned long int offset, unsigned long int length, unsigned char namespace_id) {
    if (request_t_init(req) != 0)
        return -1; 

    // name, range
    _set_file_ranged_request(req, filename, namespace_id, offset, length);
    // data
    req->file.data = data;
    req->opcode = OVERWRITE_FILE_REQ;

    return 0;
}

int set_buffered_file_partial_read_request(request_t *req, char *filename, unsigned char *data, unsigned long int offset, unsigned long int length, unsigned char namespace_id) {
    if (request_t_init(req) != 0)
        return -1; 

    // name, range
    _set_file_ranged_request(req, filename, namespace_id, offset, length);
    // data
    req->file.data = data;
    req->opcode = READ_FILE_RANGE_REQ;

}

int set_cached_file_partial_read_request(request_t *req, char *filename, char *cachepath, unsigned long int offset, unsigned long int length, unsigned char namespace_id) {
    if (request_t_init(req) != 0)
        return -1;

    // name, cache path, range
    _set_cached_file_ranged_request(req, filename, cachepath, offset, length, namespace_id);
    req->opcode = READ_FILE_RANGE_REQ;

    return 0;
}

int set_file_rename_request(request_t *req, char *old_filename, char *new_filename, unsigned char namespace_id) {
    if (request_t_init(req) != 0)
        return -1;

    // old name, new name
    _set_cached_file_request(req, old_filename, new_filename, namespace_id);
    req->opcode = RENAME_FILE_REQ;

    return 0;
}

int set_file_copy_request(request_t *req, char *src_filename, char *dst_filename, unsigned long int offset, unsigned long int length, unsigned char namespace_id) {
    // store the destination file name as the cached file path
    if (dst_filename == NULL || _set_cached_file_ranged_request(req, src_filename, dst_filename, offset, length, namespace_id) == -1)
        return -1;

    req->opcode = COPY_FILE_REQ;

    return 0;
}

int set_get_storage_capacity_request(request_t *req) {
    if (request_t_init(req) != 0)
        return -1;

    req->opcode = GET_CAPACITY_REQ;

    return 0;
}

int set_get_file_list_request(request_t *req, unsigned char namespace_id, char *prefix) {
    if (request_t_init(req) != 0)
        return -1;

    req->opcode = GET_FILE_LIST_REQ;
    req->namespace_id = namespace_id;
    req->file.filename.name = prefix == 0? "" : prefix;
    req->file.filename.length = strlen(prefix);

    return 0;
}

int set_get_append_size_request(request_t *req, char *storage_class) {
    if (request_t_init(req) != 0)
        return -1;

    req->file.storage_class.name = storage_class;
    req->file.storage_class.length = strlen(storage_class);
    req->opcode = GET_APPEND_SIZE_REQ;

    return 0;
}

int set_get_read_size_request(request_t *req, char *filename, unsigned char namespace_id) {
    if (request_t_init(req) != 0)
        return -1;

    // name
    _set_file_request(req, filename, namespace_id);
    req->opcode = GET_READ_SIZE_REQ;

    return 0;
}

int set_get_agent_status_request(request_t *req) {
    if (request_t_init(req) != 0)
        return -1;

    req->opcode = GET_AGENT_STATUS_REQ;

    return 0;
}

int set_get_proxy_status_request(request_t *req) {
    if (request_t_init(req) != 0)
        return -1;

    req->opcode = GET_PROXY_STATUS_REQ;

    return 0;
}

int set_get_repair_stats_request(request_t *req) {
    if (request_t_init(req) != 0)
        return -1;

    req->opcode = GET_REPAIR_STATS_REQ;

    return 0;
}

int set_get_background_task_progress_request(request_t *req) {
    if (request_t_init(req) != 0)
        return -1;

    req->opcode = GET_BG_TASK_PRG_REQ;

    return 0;
}


///////////////////////
//  File operations  //
///////////////////////

unsigned long int send_request(ncloud_conn_t *conn, request_t *req) {
    if (conn == NULL || req == NULL)
        return ULONG_MAX;
    // check connection setup
    if (conn->socket == NULL || conn->context == NULL)
        return ULONG_MAX;
    // send the file request
    int ret = issue_request(conn->socket, req->opcode, req->namespace_id, &req->file, &req->stats, &req->file_list, &req->agent_list, &req->proxy_status);
    if (ret < 0) {
        log_error("Failed to complete the request on file %.*s\n", req->file.filename.length, req->file.filename.name);
        return ULONG_MAX;
    } else if (
        (req->opcode == WRITE_FILE_REQ && ret != WRITE_FILE_REP_SUCCESS) ||
        (req->opcode == READ_FILE_REQ && ret != READ_FILE_REP_SUCCESS) ||
        (req->opcode == DEL_FILE_REQ && ret != DEL_FILE_REP_SUCCESS) ||
        (req->opcode == APPEND_FILE_REQ && ret != APPEND_FILE_REP_SUCCESS) ||
        (req->opcode == OVERWRITE_FILE_REQ && ret != OVERWRITE_FILE_REP_SUCCESS) ||
        (req->opcode == READ_FILE_RANGE_REQ && ret != READ_FILE_RANGE_REP_SUCCESS) ||
        (req->opcode == RENAME_FILE_REQ && ret != RENAME_FILE_REP_SUCCESS) ||
        (req->opcode == COPY_FILE_REQ && ret != COPY_FILE_REP_SUCCESS)
    ) {
        log_error("Failed to operate on file %.*s\n", req->file.filename.length, req->file.filename.name);
        return ULONG_MAX;
    }
    // decide return value
    return req->file.size;
}

static int setup_connection(const char *ip, unsigned short port, void **context, void **socket) {
    // new zmq context
    log_info("Create new context\n");
    *context = zmq_ctx_new();
    if (context == NULL) {
        log_error("Failed to create a new zero-mq context, err = %d\n", errno);
        return -1;
    }
    log_info("Create new socket\n");
    // new zmq socket
    *socket = zmq_socket(*context, ZMQ_REQ);
    if (socket == NULL) {
        log_error("Failed to create a new zero-mq socket, err = %d\n", errno);
        zmq_ctx_destroy(*context);
        return -1;
    }
    log_info("Connect to proxy\n");
    char endpoint[1024];
    snprintf(endpoint, 1024, "tcp://%s:%u", ip, port);
    int timeout = RECV_TIMEOUT;
    zmq_setsockopt(*socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    // connect to proxy
    if (zmq_connect(*socket, endpoint) != 0) {
        log_error("Failed to create a new zero-mq socket, err = %d\n", errno);
        zmq_ctx_destroy(*context);
        return -1;
    }
    log_msg("Connected to %s.\n", endpoint);
    return 0;
}

static int teardown_connection(void *context, void *socket) {
    log_info("Closing socket\n");
    zmq_close(socket);
    log_info("Closing context\n");
    zmq_ctx_destroy(context);
}

static int issue_request(void *socket, int opcode, unsigned char namespace_id, file_t *file, sys_stats_t *stats, file_list_head_t *flist, agent_info_head_t *alist, sysinfo_t *pstatus) {

#define send_field(_FIELD_, _FLAG_) (zmq_send(socket, _FIELD_, msg_length, _FLAG_) == msg_length)

    // check the input
    if (
        (!has_opcode_only(opcode) && !has_namespace_id_only(opcode) && file == NULL) ||
        (opcode == GET_CAPACITY_REQ && stats == NULL) ||
        (opcode == GET_FILE_LIST_REQ && flist == NULL && file == NULL) ||
        (opcode == GET_AGENT_STATUS_REQ && alist == NULL) ||
        (opcode == GET_PROXY_STATUS_REQ && pstatus == NULL)
    ) {
        return -1;
    }

    // send opcode
    size_t msg_length = sizeof(int);
    if (!send_field(&opcode, has_opcode_only(opcode)? 0 : ZMQ_SNDMORE)) {
        log_error("Failed to send the request opcode, err = %d\n", errno);
        return -1;
    }
    log_info("Send opcode = %d\n", opcode);

    // send namespace id
    if (!has_opcode_only(opcode)) {
        msg_length = sizeof(unsigned char);
        if (!send_field(&namespace_id, has_namespace_id_only(opcode)? 0 : ZMQ_SNDMORE)) {
            log_error("Failed to send the request namespace id, err = %d\n", errno);
            return -1;
        }
        log_info("Send namespace id = %d\n", namespace_id);
    }

    if (!has_opcode_only(opcode) && !has_namespace_id_only(opcode)) {
        if (opcode == GET_APPEND_SIZE_REQ) {
            // send file storage class
            msg_length = file->storage_class.length;
            if (!send_field(file->storage_class.name , 0)) {
                log_error("Failed to send the request file storage class, err = %d\n", errno);
                return -1;
            }
            log_info("Send file storage class = %s\n", file->storage_class.name);
        } else if (opcode == GET_READ_SIZE_REQ || opcode == GET_FILE_LIST_REQ) {
            // send file name, or file prefix
            msg_length = file->filename.length;
            if (!send_field(file->filename.name, 0)) {
                log_error("Failed to send the request file name, err = %d\n", errno);
                return -1;
            }
            log_info("Send file name = %s\n", file->filename.name);
        } else {
            // send file name
            msg_length = file->filename.length;
            if (!send_field(file->filename.name, ZMQ_SNDMORE)) {
                log_error("Failed to send the request file name, err = %d\n", errno);
                return -1;
            }
            log_info("Send file name = %s\n", file->filename.name);

            // for write request, send also the file size and file storage class
            if (has_size_or_length(opcode)) {
                // send file size
                msg_length = sizeof(file->size);
                if (!send_field(&file->size, ZMQ_SNDMORE)) {
                    log_error("Failed to send the request file size, err = %d\n", errno);
                    return -1;
                }
                log_info("Send file size = %lu\n", file->size);
            }
            if (opcode == WRITE_FILE_REQ) {
                msg_length = file->storage_class.length;
                if (!send_field(file->storage_class.name, ZMQ_SNDMORE)) {
                    log_error("Failed to send the request file storage class, err = %d\n", errno);
                    return -1;
                }
                log_info("Send file storage class = %s\n", file->storage_class.name);
            } else if (has_offset(opcode)) {
                msg_length = sizeof(file->offset);
                if (!send_field(&file->offset, ZMQ_SNDMORE)) {
                    log_error("Failed to send the request file offset, err = %d\n", errno);
                    return -1;
                }
                log_info("Send file offset= %lu\n", file->offset);
            }
            // indicate whether the file data is cached
            unsigned char is_cached = (file->cachepath.length > 0);
            msg_length = 1;
            if (!send_field(&is_cached, ((has_file_data(opcode) && file->size > 0)|| is_cached? ZMQ_SNDMORE : 0))) {
                log_error("Failed to send the request is_cached, err = %d\n", errno);
                return -1;
            }
            log_info("Send file is cache = %d\n", (int) is_cached);
            // tell the way of handling file data 
            if (is_cached) {
                // cache file name
                msg_length = file->cachepath.length;
                if (!send_field(file->cachepath.name, 0)) {
                    log_error("Failed to send the request cache name, err = %d\n", errno);
                    return -1;
                }
                log_info("Send file cache path = %s\n", file->cachepath.name);
            } else if (has_file_data(opcode) && file->size > 0){
                // file data
                msg_length = file->size;
                if (!send_field(file->data, 0)) {
                    log_error("Failed to send the request data, err = %d\n", errno);
                    return -1;
                }
                log_info("Send file data of size %lu\n", file->size);
            }
        }
    }

#undef send_field

#define get_new_msg() \
    do { \
        zmq_msg_close(&msg); \
        zmq_msg_init(&msg); \
        if (zmq_msg_recv(&msg, socket, 0) < 0) { \
            log_error("Failed to get a field in reply, err = %d\n", errno); \
            zmq_msg_close(&msg); \
            return -1; \
        } \
    } while(0)

#define get_field(_FIELD_) \
    do { \
        get_new_msg(); \
        memcpy(_FIELD_, zmq_msg_data(&msg), zmq_msg_size(&msg)); \
    } while(0)

#define check_more_msg( ) \
    if (zmq_msg_more(&msg) == 0) { \
        log_error("No more message is coming... \n"); \
        zmq_msg_close(&msg); \
        return -1; \
    }

    char timestr[100];
    time_t send_time = time(NULL);
    struct tm *info = localtime(&send_time);
    strftime(timestr, 100, "%c", info);
    log_msg("[%s] Send request = %d\n", timestr, opcode);

    zmq_msg_t msg;
    zmq_msg_init(&msg);

    // wait for reply
    int reply_opcode;
    get_field(&reply_opcode);
    log_info("Get reply opcode %d\n", reply_opcode);

    if (reply_opcode == READ_FILE_REP_SUCCESS || reply_opcode == READ_FILE_RANGE_REP_SUCCESS) {
        if (reply_opcode == READ_FILE_RANGE_REP_SUCCESS) {
            // file offset
            check_more_msg();
            get_field(&file->offset);
        }

        unsigned long int osize = file->size;

        // file size
        check_more_msg();
        get_field(&file->size);

        // file is cached
        check_more_msg();
        unsigned char is_cached;
        get_field(&is_cached);
        log_info("is request cached = %d\n", is_cached);

        check_more_msg();
        if (!is_cached) {
            unsigned char *odata = file->data;
            // use the input buffer if provided and the size is large enough 
            if (file->data && file->size > osize) {
                log_error("Failed to get data, the buffer provided is too small (%ld vs %ld)\n", osize, file->size);  
                zmq_msg_close(&msg);
                return -1;
            } else if (!file->data) { // allocate new buffer if not provided
                file->data = (unsigned char *) malloc (file->size);
            }
            if (file->data == 0) {
                log_error("Failed to allocate memory for file data, fall back to use cache (?\n");
                // TODO temporary hold the data?
                // restore the data buffer pointer (esp. if valid)
                file->data = odata;
                zmq_msg_close(&msg);
                return -1;
            } else {
                get_field(file->data);
            }
        } else {
            check_more_msg();
            get_new_msg();
            if (file->free_cachepath == 1)
                free(file->cachepath.name);
            // get length for allocating the path name first
            file->cachepath.length = zmq_msg_size(&msg);
            // allocate and copy the path name
            file->cachepath.name = (char *) malloc (file->cachepath.length + 1);
            memcpy(file->cachepath.name, zmq_msg_data(&msg), file->cachepath.length);
            file->cachepath.name[file->cachepath.length] = 0;
            // mark the path to be freed
            file->free_cachepath = 1;
        }
    } else if (reply_opcode == GET_CAPACITY_REP_SUCCESS) {
        // get storage usage
        check_more_msg();
        get_field(&stats->usage);
        // get storage capacity
        check_more_msg();
        get_field(&stats->capacity);
        // get file count 
        check_more_msg();
        get_field(&stats->file_count);
        // get file max count
        check_more_msg();
        get_field(&stats->file_limit);
    } else if (reply_opcode == GET_FILE_LIST_REP_SUCCESS) {
        // get file count
        check_more_msg();
        get_field(&flist->total);
        if (flist->total > 0) {
            flist->list = (file_list_item_t *) malloc (sizeof(file_list_item_t) * flist->total);
            if (flist->list == NULL) {
                log_error("Failed to allocate memory for file list\n");
                zmq_msg_close(&msg);
                return -1;
            }
        }
        // get the list of files
        for (unsigned int i = 0; i < flist->total; i++) {
            // allocate and copy the file name
            check_more_msg();
            get_new_msg();
            int size = zmq_msg_size(&msg);
            flist->list[i].fname = (char *) malloc (size + 1);
            if (flist->list[i].fname == NULL) {
                log_error("Failed to allocate memory for file name on file list %u\n", i);
                zmq_msg_close(&msg);
                return -1;
            }
            memcpy(flist->list[i].fname, zmq_msg_data(&msg), size);
            flist->list[i].fname[size] = 0;
            // get the file size
            check_more_msg();
            get_field(&flist->list[i].fsize);
            // get the file creation time
            check_more_msg();
            get_field(&flist->list[i].ctime);
            // get the file last access time
            check_more_msg();
            get_field(&flist->list[i].atime);
            // get the file last modified time
            check_more_msg();
            get_field(&flist->list[i].mtime);
        }
    } else if (
            reply_opcode == GET_APPEND_SIZE_REP_SUCCESS ||
            reply_opcode == GET_READ_SIZE_REP_SUCCESS
    ) {
        check_more_msg();
        get_field(&file->length);
    } else if (
            reply_opcode == APPEND_FILE_REP_SUCCESS ||
            reply_opcode == OVERWRITE_FILE_REP_SUCCESS
    ) {
        check_more_msg();
        get_field(&file->size);
    } else if (reply_opcode == GET_AGENT_STATUS_REP_SUCCESS) {
        // get the number of agents
        check_more_msg();
        get_field(&alist->total);
        if (alist->total > 0) {
            alist->list = (agent_info_t *) malloc (sizeof(agent_info_t) * alist->total);
            if (alist->list == NULL) {
                log_error("Failed to allocate memory for agent info list\n");
                zmq_msg_close(&msg);
                return -1;
            }
        }
        // get the list of agents
        for (unsigned int i = 0; i < alist->total; i++) {
            // get aliveness
            check_more_msg();
            get_field(&alist->list[i].alive);
            // get ip
            check_more_msg();
            get_new_msg();
            int addr_len = zmq_msg_size(&msg);
            alist->list[i].addr = (char*) malloc (addr_len + 1);
            memcpy(alist->list[i].addr, zmq_msg_data(&msg), addr_len);
            alist->list[i].addr[addr_len] = 0;
            // get host type
            check_more_msg();
            get_field(&alist->list[i].host_type);

#define RECV_SYS_INFO(_SYS_INFO_) \
    do { \
            /* get cpu info */ \
            check_more_msg(); \
            get_field(&(_SYS_INFO_->cpu.num)); \
            check_more_msg(); \
            get_new_msg(); \
            memcpy(_SYS_INFO_->cpu.usage, zmq_msg_data(&msg), _SYS_INFO_->cpu.num * sizeof(float)); \
            /* get memory info */ \
            check_more_msg(); \
            get_field(&(_SYS_INFO_->mem.total)); \
            check_more_msg(); \
            get_field(&(_SYS_INFO_->mem.free)); \
            /* get network info */ \
            check_more_msg(); \
            get_field(&(_SYS_INFO_->net.in)); \
            check_more_msg(); \
            get_field(&(_SYS_INFO_->net.out)); \
            /* get host type info */ \
            check_more_msg(); \
            get_field(&(_SYS_INFO_->host_type)); \
    } while(0)

            sysinfo_t *info = &(alist->list[i].sysinfo);
            RECV_SYS_INFO(info);

            // get the number of containers
            check_more_msg();
            get_field(&alist->list[i].num_containers);
            if (alist->list[i].num_containers > 0) {
                // get the container ids
                check_more_msg();
                get_field(alist->list[i].container_id);
                // get the container type
                check_more_msg();
                get_field(alist->list[i].container_type);
                // get the container usage 
                check_more_msg();
                get_field(alist->list[i].container_usage);
                // get the container capacity 
                check_more_msg();
                get_field(alist->list[i].container_capacity);
            }
        }
    } else if (reply_opcode == GET_PROXY_STATUS_REP_SUCCESS) {
        RECV_SYS_INFO(pstatus);
#undef RECV_SYS_INFO
    } else if (reply_opcode == GET_BG_TASK_PRG_REP_SUCCESS) {
        // get file count
        check_more_msg();
        get_field(&flist->total);
        if (flist->total > 0) {
            flist->list = (file_list_item_t *) malloc (sizeof(file_list_item_t) * flist->total);
            if (flist->list == NULL) {
                log_error("Failed to allocate memory for task list\n");
                zmq_msg_close(&msg);
                return -1;
            }
        }
        // get the list of files
        for (unsigned int i = 0; i < flist->total; i++) {
            // allocate and copy the (task) file name
            check_more_msg();
            get_new_msg();
            int size = zmq_msg_size(&msg);
            flist->list[i].fname = (char *) malloc (size + 1);
            if (flist->list[i].fname == NULL) {
                log_error("Failed to allocate memory for file name on file list %u\n", i);
                zmq_msg_close(&msg);
                return -1;
            }
            memcpy(flist->list[i].fname, zmq_msg_data(&msg), size);
            flist->list[i].fname[size] = 0;
            // get the progress
            check_more_msg();
            int progress = 0;
            get_field(&progress);
            flist->list[i].fsize = progress;
        }
    } else if (reply_opcode == GET_REPAIR_STATS_REP_SUCCESS) {
        // get file count 
        check_more_msg();
        get_field(&stats->file_count);
        // get repair count (as the file limit count)
        check_more_msg();
        get_field(&stats->file_limit);
    }

    zmq_msg_close(&msg);

#undef get_field
#undef get_new_msg
#undef check_more_msg

    time_t complete_time = time(NULL);
    info = localtime(&complete_time);
    strftime(timestr, 100, "%c", info);
    log_msg("[%s] Complete request = %d and reply = %d\n", timestr, opcode, reply_opcode);

    return reply_opcode;
}

static int has_file_data(int opcode) {
    return (
        opcode == WRITE_FILE_REQ ||
        opcode == APPEND_FILE_REQ ||
        opcode == OVERWRITE_FILE_REQ
    );
}

static int has_opcode_only(int opcode) {
    return (
        opcode == GET_CAPACITY_REQ ||
        opcode == GET_AGENT_STATUS_REQ ||
        opcode == GET_PROXY_STATUS_REQ ||
        opcode == GET_BG_TASK_PRG_REQ
    );
}

static int has_namespace_id_only(int opcode) {
    return 0;
}

static int has_size_or_length(int opcode) {
    return (
        opcode == WRITE_FILE_REQ ||
        opcode == APPEND_FILE_REQ ||
        opcode == OVERWRITE_FILE_REQ ||
        opcode == READ_FILE_RANGE_REQ ||
        opcode == COPY_FILE_REQ
    );
}

static int has_offset(int opcode) {
    return (
        opcode == APPEND_FILE_REQ ||
        opcode == OVERWRITE_FILE_REQ ||
        opcode == READ_FILE_RANGE_REQ ||
        opcode == COPY_FILE_REQ
    );
}
