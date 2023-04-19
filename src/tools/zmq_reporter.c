// SPDX-License-Identifier: Apache-2.0

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

#include <json-c/json.h>
#include <glib.h>
#include <glib/gprintf.h>
#include <getopt.h>
#include <hiredis/hiredis.h>

#include "../client/c/zmq_interface.h"
#include "../common/define.hh"

#define HUMAN_BYTE_STRING_LEN      (128)
#define TEST_NAMESPACE_ID          (-1)
#define LOCAL_HOST                 "127.0.0.1"

enum InfoIdx{
    STATUS = 0,
    PENDING_REPAIR,
    BG_TASKS,
    STORAGE_CAP,
};

const char *history[] = {
    "ncloud_status_hist",
    "ncloud_repair_hist",
    "ncloud_bgtasks_hist",
    "ncloud_storage_hist",
};

const char *channel[] = {
    "ncloud_status",
    "ncloud_repair",
    "ncloud_bgtasks",
    "ncloud_storage",
};

const char *host_types[] = {
    "On-prem",
    "Alibaba",
    "AWS",
    "Azure",
    "Tencent",
    "Google",
    "Huawei",

    "Unknown"
};

// connections (ncloud, redis)
ncloud_conn_t conn;
redisContext *_cxt = 0;

// modes
int verbose = 1;
int send_to_redis = 0;

// op timestamp
time_t time_now = 0;
const char *proxy_ip = 0;

// convert bytes to human-readable format (KB, MB, GB, TB, PB)
void convert_to_human_bytes(unsigned long int bytes, char *buf) {
    if (buf == NULL) return;
    if (bytes < ((unsigned long int) 1 << 10)) // B
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%6luB", bytes);
    else if (bytes < ((unsigned long int) 1 << 20)) // KB
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%6.2lfKB", bytes * 1.0 / ((unsigned long int) 1 << 10));
    else if (bytes < ((unsigned long int) 1 << 30)) // MB
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%6.2lfMB", bytes * 1.0 / ((unsigned long int) 1 << 20));
    else if (bytes < ((unsigned long int) 1 << 40)) // GB
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%6.2lfGB", bytes * 1.0 / ((unsigned long int) 1 << 30));
    else if (bytes < ((unsigned long int) 1 << 50)) // TB
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%6.2lfTB", bytes * 1.0 / ((unsigned long int) 1 << 40));
    else // PB
        snprintf(buf, HUMAN_BYTE_STRING_LEN, "%6.2lfPB", bytes * 1.0 / ((unsigned long int) 1 << 50));
}

// convert timestamp to seconds
double convert_to_seconds(const struct timeval t) {
    return t.tv_sec * 1.0 + t.tv_usec / 1e-6;
}

// wrap printing to stdout for verbose mode
int my_printf(const char *format, ...) {
    if (verbose) {
        va_list args;
        va_start(args, format);
        vprintf(format, args);
        va_end(args);
    }
}

#ifndef NDEBUG
    #define my_debug my_printf
#else
int my_debug() {}
#endif /* ifndef NDEBUG */

int push_data_to_redis(const char *key, char *command, json_object *obj) {
    if (key == NULL || command == NULL || obj == NULL)
        return 1;
        
    redisReply *rep = (redisReply *) redisCommand(
            _cxt
            , "%s %s %s"
            , command
            , key 
            , json_object_to_json_string(obj)
        );

    if (rep != NULL) {
        if (strcmp(command, "PUBLISH") == 0)
            my_debug("Data published to channel %s with %d clients received\n", key, rep->integer);
        else if (strcmp(command, "RPUSH") == 0 || strcmp(command, "LPUSH") == 0)
            my_debug("Data pushed to list %s at position", key, rep->integer);
    } else {
        if (strcmp(command, "PUBLISH") == 0)
            fprintf(stderr, "Failed to publish data to channel %s", key);
        else if (strcmp(command, "RPUSH") == 0 || strcmp(command, "LPUSH") == 0)
            fprintf(stderr, "Failed to push data to list %s", key);
    }

    freeReplyObject(rep);

    return rep != NULL;
}

// publish json to a channel in Redis
int publish_to_redis(const char *channel, json_object *obj) {
    return push_data_to_redis(channel, "PUBLISH", obj);
}

// save json to a list in Redis
int save_to_redis(const char *list, json_object *obj) {
    return push_data_to_redis(list, "RPUSH", obj);
}

// obtain system status, print for verbose mode, publish and save to Redis if needed
int get_sys_status() {
    request_t req;

    // get proxy status
    json_object *obj = 0, *proxy = 0;
    json_object *cpu = 0, *mem = 0, *net = 0, *container = 0;
    json_object *cpu_list = 0;

    set_get_proxy_status_request(&req);
    if (send_request(&conn, &req) == -1)
        return 1;

    if (send_to_redis) {
        // root
        obj = json_object_new_object();
        // 1st level
        json_object_object_add(obj, "ts", json_object_new_int64(time_now));
        proxy = json_object_new_object();
        // 2nd level
        cpu = json_object_new_object();
        mem = json_object_new_object();
        net = json_object_new_object();
        json_object_object_add(cpu, "num", json_object_new_int(req.proxy_status.cpu.num));
        json_object_object_add(mem, "total", json_object_new_int(req.proxy_status.mem.total));
        json_object_object_add(mem, "free", json_object_new_int(req.proxy_status.mem.free));
        json_object_object_add(net, "in", json_object_new_double(req.proxy_status.net.in));
        json_object_object_add(net, "out", json_object_new_double(req.proxy_status.net.out));
        // 3rd level
        cpu_list = json_object_new_array();
    }

    my_printf("> Proxy [%-7s] CPU (%d"
        , host_types[req.proxy_status.host_type]
        , req.proxy_status.cpu.num
    );

    for (int j = 0; j < req.proxy_status.cpu.num; j++) {
        my_printf(", %f", req.proxy_status.cpu.usage[j]);
        if (send_to_redis)
            json_object_array_add(cpu_list, json_object_new_double(req.proxy_status.cpu.usage[j]));
    }

    my_printf(") Mem (used/total) %dMB/%dMB Net RX %.2lfB/s TX %.2lfB/s\n"
            , req.proxy_status.mem.total - req.proxy_status.mem.free
            , req.proxy_status.mem.total
            , req.proxy_status.net.in
            , req.proxy_status.net.out
    );

    if (send_to_redis) {
        // 3rd level
        json_object_object_add(cpu, "usage", cpu_list);
        // 2nd level
        json_object_object_add(proxy, "cpu", cpu);
        json_object_object_add(proxy, "mem", mem);
        json_object_object_add(proxy, "net", net);
        json_object_object_add(proxy, "ip", json_object_new_string(proxy_ip));
        json_object_object_add(proxy, "host_type", json_object_new_string(host_types[req.proxy_status.host_type]));
        // 1st level
        json_object_object_add(obj, "proxy", proxy);
    }

    request_t_release(&req);


    // get agents' status
    set_get_agent_status_request(&req);
    if (send_request(&conn, &req) == -1)
        return 1;

    json_object *agent = 0, *agents = 0, *agent_objs[req.agent_list.total];
    json_object *container_list = 0;
    // json to Redis
    if (send_to_redis) {
        agent = json_object_new_object();
        agents = json_object_new_array();
        json_object_object_add(agent, "total", json_object_new_int(req.agent_list.total));
    }

    my_printf("> Proxy is connected to %u Agent%s\n", req.agent_list.total, req.agent_list.total > 1? "s" : "");

    char usage[HUMAN_BYTE_STRING_LEN], capacity[HUMAN_BYTE_STRING_LEN];

    for (unsigned int i = 0; i < req.agent_list.total; i++) {
        if (send_to_redis) {
            agent_objs[i] = json_object_new_object();
            json_object_object_add(agent_objs[i], "alive", json_object_new_boolean(req.agent_list.list[i].alive));
            json_object_object_add(agent_objs[i], "host_type", json_object_new_string(host_types[req.agent_list.list[i].host_type]));
            json_object_object_add(agent_objs[i], "ip", json_object_new_string(req.agent_list.list[i].addr));
            json_object_object_add(agent_objs[i], "num_containers", json_object_new_int(req.agent_list.list[i].num_containers));

            cpu = json_object_new_object();
            mem = json_object_new_object();
            net = json_object_new_object();
            cpu_list = json_object_new_array();
            container_list = json_object_new_array();

            json_object_object_add(cpu, "num", json_object_new_int(req.agent_list.list[i].sysinfo.cpu.num));
            json_object_object_add(mem, "total", json_object_new_int(req.agent_list.list[i].sysinfo.mem.total));
            json_object_object_add(mem, "free", json_object_new_int(req.agent_list.list[i].sysinfo.mem.free));
            json_object_object_add(net, "in", json_object_new_double(req.agent_list.list[i].sysinfo.net.in));
            json_object_object_add(net, "out", json_object_new_double(req.agent_list.list[i].sysinfo.net.out));
        }
        my_printf(
                "  %3d. Agent [%-12s] at %s (%-7s) with %2d containers\n"
                , i + 1
                , req.agent_list.list[i].alive? "\e[1;32mALIVE\e[m" : "\e[1;31mDISCONNECTED\e[m"
                , req.agent_list.list[i].addr
                , host_types[req.agent_list.list[i].host_type]
                , req.agent_list.list[i].num_containers
        );
        my_printf("       CPU (%d"
                , req.agent_list.list[i].sysinfo.cpu.num
        );
        for (int j = 0; j < req.agent_list.list[i].sysinfo.cpu.num; j++) {
            my_printf(", %f", req.agent_list.list[i].sysinfo.cpu.usage[j]);
            if (send_to_redis)
                json_object_array_add(cpu_list, json_object_new_double(req.agent_list.list[i].sysinfo.cpu.usage[j]));
        }
        my_printf(") Memory %dMB/%dMB Net TX %.2lfB/RX %.2lfB\n"
                , req.agent_list.list[i].sysinfo.mem.total - req.agent_list.list[i].sysinfo.mem.free
                , req.agent_list.list[i].sysinfo.mem.total
                , req.agent_list.list[i].sysinfo.net.in
                , req.agent_list.list[i].sysinfo.net.out
        );

        for (int j = 0; j < req.agent_list.list[i].num_containers; j++) {
            convert_to_human_bytes(req.agent_list.list[i].container_usage[j], usage);
            convert_to_human_bytes(req.agent_list.list[i].container_capacity[j], capacity);
            my_printf("      Container [%3d] [%-7s], %13lu/%13lu (%s/%s), %.2f%% used\n",
                    req.agent_list.list[i].container_id[j],
                    host_types[req.agent_list.list[i].container_type[j]],
                    req.agent_list.list[i].container_usage[j],
                    req.agent_list.list[i].container_capacity[j],
                    usage,
                    capacity,
                    req.agent_list.list[i].container_usage[j] * 100.0 / req.agent_list.list[i].container_capacity[j]
            );
            if (send_to_redis) {
                container = json_object_new_object();
                json_object_object_add(container, "id", json_object_new_int(req.agent_list.list[i].container_id[j]));
                json_object_object_add(container, "type", json_object_new_string(host_types[req.agent_list.list[i].container_type[j]]));
                json_object_object_add(container, "usage", json_object_new_int64(req.agent_list.list[i].container_usage[j]));
                json_object_object_add(container, "capacity", json_object_new_int64(req.agent_list.list[i].container_capacity[j]));
                json_object_array_add(container_list, container);
            }
        }

        if (send_to_redis) {
            json_object_object_add(cpu, "usage", cpu_list);
            json_object_object_add(agent_objs[i], "cpu", cpu);
            json_object_object_add(agent_objs[i], "mem", mem);
            json_object_object_add(agent_objs[i], "net", net);
            json_object_object_add(agent_objs[i], "containers", container_list);
            json_object_array_add(agents, agent_objs[i]);
        }
    }

    if (send_to_redis) {
        // 2nd level
        json_object_object_add(agent, "agents", agents);
        // 1st level
        json_object_object_add(obj, "agent", agent);
        publish_to_redis(channel[STATUS], obj);
        save_to_redis(history[STATUS], obj);
        json_object_put(obj);
    }

    request_t_release(&req);

    return 0;
}

// obtain the number of backgrond tasks , print for verbose mode, publish and save to Redis if needed
int get_bg_task_progress() {
    request_t req;
    set_get_background_task_progress_request(&req);
    if (send_request(&conn, &req) == -1)
        return 1;

    if (send_to_redis) {
        json_object *obj = json_object_new_object();
        json_object_object_add(obj, "ts", json_object_new_int64(time_now));
        json_object_object_add(obj, "total", json_object_new_int(req.file_list.total));

        publish_to_redis(channel[BG_TASKS], obj);
        save_to_redis(history[BG_TASKS], obj);

        json_object_put(obj);
    }

    my_printf("> Number of on-going background tasks: %d\n", req.file_list.total);
    for (unsigned int i = 0; i < req.file_list.total; i++)
        my_printf("  %3d. %-20s: %lu%%\n", i+1, req.file_list.list[i].fname, req.file_list.list[i].fsize);;

    request_t_release(&req);
    return 0;
}

// obtain the number of pending repairs, print for verbose mode, publish and save to Redis if needed
int get_repair_progress() {
    request_t req;
    set_get_repair_stats_request(&req);
    if (send_request(&conn, &req) == -1)
        return 1;

    // json to Redis
    if (send_to_redis) {
        json_object *obj = json_object_new_object();
        json_object_object_add(obj, "ts", json_object_new_int64(time_now));
        json_object_object_add(obj, "total", json_object_new_int(req.stats.file_limit));

        publish_to_redis(channel[PENDING_REPAIR], obj);
        save_to_redis(history[PENDING_REPAIR], obj);
        json_object_put(obj);
    }


    if (req.stats.file_limit > 0)
        my_printf("> Number of files pending for repair or under repair: %lu\n", req.stats.file_limit);
    else
        my_printf("> No files pending for repair\n");
        

    request_t_release(&req);
    return 0;
}

// obtain the storage capacity, print for verbose mode, publish and save to Redis if needed
int get_storage_capacity() {
    request_t req;
    set_get_storage_capacity_request(&req);
    if (send_request(&conn, &req) == -1)
        return 1;

    char usage[HUMAN_BYTE_STRING_LEN], capacity[HUMAN_BYTE_STRING_LEN];

    // json to Redis
    if (send_to_redis) {
        json_object *obj = json_object_new_object();
        json_object_object_add(obj, "ts", json_object_new_int64(time_now));
        json_object_object_add(obj, "usage", json_object_new_int64(req.stats.usage));
        json_object_object_add(obj, "capacity", json_object_new_int64(req.stats.capacity));

        publish_to_redis(channel[STORAGE_CAP], obj);
        save_to_redis(history[STORAGE_CAP], obj);
        json_object_put(obj);
    }

    convert_to_human_bytes(req.stats.usage, usage);
    convert_to_human_bytes(req.stats.capacity, capacity);

    my_printf(
        "> Storage usage = %13lu/%13lu (%s/%s, %6.2f%% free)\n"
        , req.stats.usage
        , req.stats.capacity
        , usage
        , capacity
        , (req.stats.capacity >= req.stats.usage? (req.stats.capacity - req.stats.usage) * 100.0 / req.stats.capacity : 0)
    );

    request_t_release(&req);
    return 0;
}

// report usage
void print_usage(char *prog) {
    fprintf(stderr, 
            "Usage: %s [OPTIONS] [config file directory]\n"
            "\n"
            "  -s<interval>      run as server and report every 'interval' seconds\n"
            "  -r[redis url]     save the report to redis (IP:port)\n"
            "  -q                quite mode (do not print to stdout)\n"
            "  -h                print this help message\n"
            "\n"
            , prog 
    );
}


int main(int argc, char **argv) {
    g_autoptr(GError) error = 0; 
    g_autoptr(GKeyFile) proxy_config = g_key_file_new();
    g_autoptr(GKeyFile) general_config = g_key_file_new();

    int read_db_addr_from_config = 0;

    // parse options
    int opt, report_interval = 0, count = 0;
    char redis_ip[32];
    unsigned short redis_port;
    char *tmp = NULL;
    redis_ip[0] = 0;
    while ((opt = getopt(argc, argv, "r::s:hq")) != -1) {
        switch(opt) {
            case 's':
                report_interval = atoi(optarg);
                count++;
                break;
            case 'r':
                if (optarg) {
                    tmp = strstr(optarg, ":");
                    if (tmp == NULL) {
                        redis_port = 6379;
                        tmp = optarg + strlen(optarg);
                    } else {
                        redis_port = atoi(tmp + 1);
                    }
                    strncpy(redis_ip, optarg, tmp - optarg);
                    redis_ip[tmp - optarg] = 0;
                    //printf("Connect to Redis at %s port %u \n", redis_ip, redis_port);
                } else {
                    read_db_addr_from_config = 1;
                }
                count++;
                break;
            case 'q':
                verbose = 0;
                count++;
                break;
            case 'h':
            default:
                print_usage(argv[0]);
                return opt != 'h';
        }
    }

    // read config
    int no_config = count + 1 == argc;
    char proxy_path[PATH_MAX], general_path[PATH_MAX];
    snprintf(proxy_path, PATH_MAX, "%s/%s", no_config? "." : argv[argc - 1], "proxy.ini");
    snprintf(general_path, PATH_MAX, "%s/%s", no_config? "." : argv[argc - 1], "general.ini");
    
    if (!g_key_file_load_from_file(proxy_config, proxy_path, G_KEY_FILE_NONE, &error)) {
        fprintf(stderr, "Failed to load proxy.ini at path %s, %s\n", proxy_path, error->message);
        return 1;
    }

    if (!g_key_file_load_from_file(general_config, general_path, G_KEY_FILE_NONE, &error)) {
        fprintf(stderr, "Failed to load general.ini at path %s, %s\n", general_path, error->message);
        return 1;
    }

    // get reporter db address from config if not provided
    if (read_db_addr_from_config) {
        const char *reporter_db_key = "reporter_db";
        gchar *config_redis_ip = g_key_file_get_string(proxy_config, reporter_db_key, "ip", &error);
        if (!error || strlen(config_redis_ip) > 0) {
            strcpy(redis_ip, config_redis_ip);
            redis_port = g_key_file_get_integer(proxy_config, reporter_db_key, "port", &error);
            if (error)
                redis_ip[0] = 0;
        }
        if (error || strlen(config_redis_ip) == 0) {
            fprintf(stderr, "Failed to load reporter db from config file %s, %s\n", proxy_path, error? error->message : "no IP provided in config file");
        }
        g_free(config_redis_ip);
    }

    // determine whether to save and publish to redis
    send_to_redis = strlen(redis_ip) > 0;

    // get the proxy information for connecting zero-mq interface
    gint proxy_num = g_key_file_get_integer(proxy_config, "proxy", "num", &error);
    char proxy_key[32];
    snprintf(proxy_key, 32, "proxy%02d", proxy_num);
    error = 0;
    gchar *ip = g_key_file_get_string(general_config, proxy_key, "ip", &error);
    if (error)
        ip = LOCAL_HOST;
    error = 0;
    gint port = g_key_file_get_integer(proxy_config, "zmq_interface", "port", &error);
    if (error)
        port = 59001;

    // mark the proxy ip (hard-code for single proxy; TODO support multiple proxies by querying to all of them)
    proxy_ip = ip;

    // init ncloud connection
    ncloud_conn_t_init(ip, port, &conn, 1);
    int okay = 0;

    // init redis connection
    if (send_to_redis) {
        struct timeval timeout = {7, 0};
        _cxt = redisConnectWithTimeout(redis_ip, redis_port, timeout);
        if (_cxt == NULL || _cxt->err) {
            if (_cxt) 
                redisFree(_cxt);
            fprintf(stderr, "Failed to connect to Redis at %s:%hu, %s\n", redis_ip, redis_port, _cxt? _cxt->errstr : "(nil)");
            return 1;
        } else {
            //printf("Connect to Redis at %s port %u \n", redis_ip, redis_port);
        }
    }

    // generate report(s)
    do {
        // clean screen
        if (report_interval > 0)
            my_printf("\e[1;1H\e[2J");

        time_now = time(NULL);
        my_printf("Time: %s", ctime(&time_now));
        my_printf("================================================================================\n");
        if (get_sys_status() == -1) {
            okay = 1;
            fprintf(stderr, "Failed to get System status!\n");
        }

        if (get_bg_task_progress() == -1) {
            okay = 1;
            fprintf(stderr, "Failed to get Background Task progress!\n");
        }

        if (get_repair_progress() == -1) {
            okay = 1;
            fprintf(stderr, "Failed to get Repair progress!\n");
        }

        if (get_storage_capacity() == -1) {
            okay = 1;
            fprintf(stderr, "Failed to get Storage capacity!\n");
        }
        my_printf("================================================================================\n");
            
        // sleep until next scheduled report time
        sleep(report_interval);

    } while (report_interval > 0);

    // close redis connection
    redisFree(_cxt);

    // close ncloud connection
    ncloud_conn_t_release(&conn);

    g_free(ip);

    return okay == 0;
}
