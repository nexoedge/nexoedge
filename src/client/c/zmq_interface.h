// SPDX-License-Identifier: Apache-2.0

#ifndef __ZMQ_CLIENT_INTERFACE_H__
#define __ZMQ_CLIENT_INTERFACE_H__

#include <time.h>
#include <stdlib.h>

#include "../../common/zmq_int_define.hh"
#define MAX_NUM_CONTAINER_PER_AGENT (16)
#define UNKNOWN_NAMESPACE_ID ((unsigned char) -1)

typedef struct {
    char *name;                   /**< the name */
    int length;                   /**< length of the name */
} name_t;

typedef struct {
    name_t filename;              /**< file name */
    name_t cachepath;             /**< cache file path */
    unsigned long int offset;     /**< file offset (for append) */
    union {
        unsigned long int size;   /**< file size */
        unsigned long int length; /**< data length(for append) */
    };
    name_t storage_class;         /**< storage class (for write)*/
    unsigned char *data;          /**< file data */

    int free_cachepath;           /**< whether the path name needs to be freed upon release */
    int free_filename;            /**< whether the file name needs to be freed upon release */
    int free_data;                /**< whether the data needs to be freed upon release */
    int is_data_mmap;             /**< whether the data is mmaped */
} file_t;

typedef struct {
    struct {
        char num;                 /**< num of cpus */
        float usage[256];         /**< cpu usage in percentage */
    } cpu;
    struct {
        unsigned int total;       /**< total memory size (MB) */
        unsigned int free;        /**< free memory size (MB) */
    } mem;
    struct {
        double in;                /**< ingress traffic rate */
        double out;               /**< egress traffic rate */
    } net;

    unsigned char host_type;      /**< host type */
} sysinfo_t;

typedef struct {
    unsigned char alive;                                               /**< whether the agent is alive */
    char *addr;                                                        /**< address */
    unsigned char host_type;                                           /**< host type */
    int num_containers;                                                /**< number of containers managed by agent */
    int container_id[MAX_NUM_CONTAINER_PER_AGENT];                     /**< ids of containres */
    unsigned long int container_usage[MAX_NUM_CONTAINER_PER_AGENT];    /**< storage usage of containers managed by agent */
    unsigned long int container_capacity[MAX_NUM_CONTAINER_PER_AGENT]; /**< storage capacity of containers managed by agent */
    unsigned char container_type[MAX_NUM_CONTAINER_PER_AGENT];         /**< storage type of containers managed by agent */
    sysinfo_t sysinfo;                                                 /**< agent system status */
} agent_info_t;

typedef struct {
    unsigned long int usage;      /**< storage usage */
    unsigned long int capacity;   /**< storage capacity*/
    unsigned long int file_count; /**< current number of files */
    unsigned long int file_limit; /**< max number of files */
} sys_stats_t;

typedef struct {
    char *fname;
    unsigned long int fsize;
    time_t ctime;
    time_t atime;
    time_t mtime;
} file_list_item_t;

typedef struct {
    file_list_item_t *list;
    unsigned int total;
} file_list_head_t;

typedef struct {
    agent_info_t *list;
    unsigned int total;
} agent_info_head_t;

typedef struct {
    int opcode;                   /**< code of the file operation */
    unsigned char namespace_id;   /**< namespace id of the client */
    file_t file;                  /**< file */
    sys_stats_t stats;            /**< system stats */
    file_list_head_t file_list;   /**< file list */
    agent_info_head_t agent_list; /**< agent list */
    sysinfo_t proxy_status;       /**< proxy status */
} request_t;

typedef struct {
    void *socket;
    void *context;
} ncloud_conn_t;

// name init and release helpers
int name_t_init(name_t *name);
void name_t_release(name_t *name);

// file information init and release helpers
int file_t_init(file_t *file);
void file_t_release(file_t *file);

// file list init and release helpers
int file_list_item_t_init(file_list_item_t *fitem);
void file_list_item_t_release(file_list_item_t *fitem);
int file_list_head_t_init(file_list_head_t *head);
void file_list_head_t_release(file_list_head_t *head);

// system info init and release helpers
int sysinfo_t_init(sysinfo_t *sysinfo);
void sysinfo_t_release(sysinfo_t *sysinfo);

// agent status init and release helpers
int agent_info_t_init(agent_info_t *ainfo);
void agent_info_t_release(agent_info_t *ainfo);
int agent_info_head_t_init(agent_info_head_t *head);
void agent_info_head_t_release(agent_info_head_t *head);

// stats information init and release helpers
int sys_stats_t_init(sys_stats_t *stats);
void sys_stats_t_release(sys_stats_t *stats);

// request init and release helpers
int request_t_init(request_t *request);
void request_t_release(request_t *requeset);

// ncloud conn init and release helpers
int ncloud_conn_t_init(const char *ip, unsigned short port, ncloud_conn_t *conn, int connect);
void ncloud_conn_t_release(ncloud_conn_t *conn);

// system (metadata) operations
int set_get_storage_capacity_request(request_t *req);
int set_get_file_list_request(request_t *req, unsigned char namespace_id, char *preifx);
int set_get_agent_status_request(request_t *req);
int set_get_proxy_status_request(request_t *req);
int set_get_repair_stats_request(request_t *req);
int set_get_background_task_progress_request(request_t *req);

// file (metadata) operations

// file (data) operations
int set_buffered_file_write_request(request_t *req, char *filename, unsigned long int filesize, unsigned char *data, char *storage_class, unsigned char namespace_id);
int set_cached_file_write_request(request_t *req, char *filename, unsigned long int filesize, char *cachepath, char *storage_class, unsigned char namespace_id);
int set_buffered_file_read_request(request_t *req, char *filename, unsigned char namespace_id);
int set_cached_file_read_request(request_t *req, char *filename, char *cachepath, unsigned char namespace_id);
int set_delete_file_request(request_t *req, char *filename, unsigned char namespace_id);
int set_buffered_file_append_request(request_t *req, char *filename, unsigned char *data, unsigned long int offset, unsigned long int length, unsigned char namespace_id);
int set_cached_file_append_request(request_t *req, char *filename, char *cachepath, unsigned long int offset, unsigned long int length, unsigned char namespace_id);
int set_buffered_file_overwrite_request(request_t *req, char *filename, unsigned char *data, unsigned long int offset, unsigned long int length, unsigned char namespace_id);
int set_buffered_file_partial_read_request(request_t *req, char *filename, unsigned char *data, unsigned long int offset, unsigned long int length, unsigned char namespace_id);
int set_cached_file_partial_read_request(request_t *req, char *filename, char *cachepath, unsigned long int offset, unsigned long int length, unsigned char namespace_id);
int set_file_rename_request(request_t *req, char *old_filename, char *new_filename, unsigned char namespace_id);
int set_file_copy_request(request_t *req, char *src_filename, char *dst_filename, unsigned long int offset, unsigned long int length, unsigned char namespace_id);
int set_get_append_size_request(request_t *req, char *storage_class);
int set_get_read_size_request(request_t *req, char *filename, unsigned char namespace_id);

/**
 * Send a request to Proxy and wait for reply
 *
 * @param[in] conn            the connection properly init by the ncloud_conn_t_init()
 * @param[in] request         the request properly init by the set_*_request()
 * @return ULONG_MAX if failed, size field set by response on success 
 **/
unsigned long int send_request(ncloud_conn_t *conn, request_t *request);

#endif //define __PROXY_ZMQ_INT_H__
