/*
 * Unix socket protocol
 * Copyright (c) 2013 Luca Barbato
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

/**
 * @file
 *
 * Unix socket url_protocol
 */

#include "libavutil/avstring.h"
#include "libavutil/opt.h"
#include "os_support.h"
#include "network.h"
#include <sys/un.h>
#include <inttypes.h>
#include "url.h"
#include <stdatomic.h>

typedef struct UnixYContext {
    const AVClass *class;
    struct sockaddr_un addr;
    int addr_len;
    int timeout;
    // int listen;
    // int type;
    // int seekable;
    int control_fd;
    int cur_fd;
    int reading_mode;
    int session_id;
    int64_t pos; 
} UnixYContext;

#define OFFSET(x) offsetof(UnixYContext, x)
#define ED AV_OPT_FLAG_DECODING_PARAM|AV_OPT_FLAG_ENCODING_PARAM
static const AVOption unix_y_options[] = {
    // { "listen",    "Open socket for listening",             OFFSET(listen),  AV_OPT_TYPE_BOOL,  { .i64 = 0 },                    0,       1, ED },
    { "timeout",   "Timeout in ms",                         OFFSET(timeout), AV_OPT_TYPE_INT,   { .i64 = -1 },                  -1, INT_MAX, ED },
    // { "type",      "Socket type",                           OFFSET(type),    AV_OPT_TYPE_INT,   { .i64 = SOCK_STREAM },    INT_MIN, INT_MAX, ED, "type" },
    // { "seekable",  "Seekable",                              OFFSET(seekable),AV_OPT_TYPE_BOOL,  { .i64 = 0 },                    0,       1, ED },
    // { "stream",    "Stream (reliable stream-oriented)",     0,               AV_OPT_TYPE_CONST, { .i64 = SOCK_STREAM },    INT_MIN, INT_MAX, ED, "type" },
    // { "datagram",  "Datagram (unreliable packet-oriented)", 0,               AV_OPT_TYPE_CONST, { .i64 = SOCK_DGRAM },     INT_MIN, INT_MAX, ED, "type" },
    // { "seqpacket", "Seqpacket (reliable packet-oriented",   0,               AV_OPT_TYPE_CONST, { .i64 = SOCK_SEQPACKET }, INT_MIN, INT_MAX, ED, "type" },
    { NULL }
};

static const AVClass unix_y_class = {
    .class_name = "unix_y",
    .item_name  = av_default_item_name,
    .option     = unix_y_options,
    .version    = LIBAVUTIL_VERSION_INT,
};


static int write_str(int fd, char * buf, int len) {
    int write = 0;
    while (write < len) {
        int ret = send(fd, &buf[write], len - write, MSG_NOSIGNAL);
        if (ret == 0) return AVERROR(ENETDOWN); // stream closed
        if (ret < 0) return AVERROR(errno);
        write += ret;
    }
    return 0;
}

static int read_str_until(int fd, char until_char, char * out, int max) {
    char buf[1];
    int read = 0;
    while (read < max) {
        int ret = recv(fd, buf, 1, MSG_WAITALL);
        if (ret < 0) return ff_neterrno();
        if (ret < 1) return AVERROR_EOF;
        if (buf[0] == until_char) {
            out[read] = '\0';
            break;
        } else {
            out[read] = buf[0];
        }
        read++;
    }
    return read;
}

static int connectSocket(URLContext *h) {
    av_log(h, AV_LOG_DEBUG, "Opening new connection\n");
    UnixYContext *s = h->priv_data;
    int fd, ret;
    if ((fd = ff_socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
        return ff_neterrno();
    }
    ret = ff_listen_connect(fd, (struct sockaddr *)&s->addr,
                            s->addr_len, s->timeout, h, 0);
    
    if (ret < 0) {
        av_log(h, AV_LOG_DEBUG, "Close %d\n", fd);
        closesocket(fd);
        return ret;
    }

    // disable non-blocking
    ff_socket_nonblock(fd, 0);

    static atomic_int session_id_inc = ATOMIC_VAR_INIT(0);

    if (s->session_id > 0) {
        char cmd[50];
        av_log(h, AV_LOG_INFO, "Run session %d\n", s->session_id);
        sprintf(cmd, "run_session %d\n", s->session_id);
        if ((ret = write_str(fd, cmd, strlen(cmd))) < 0) {
            av_log(h, AV_LOG_ERROR, "Close %d because run_session failed\n", fd);
            closesocket(fd);
            return ret;
        }
    } else {
        char cmd[50];
        atomic_fetch_add(&session_id_inc, 1);
        int new_session_id = atomic_load(&session_id_inc);
        if (s->reading_mode) {
            av_log(h, AV_LOG_INFO, "Create session read %d\n", new_session_id);
            sprintf(cmd, "new_session read %d\n", new_session_id);
        } else {
            av_log(h, AV_LOG_INFO, "Create session write %d\n", new_session_id);
            sprintf(cmd, "new_session write %d\n", new_session_id);
        }
        if ((ret = write_str(fd, cmd, strlen(cmd))) < 0) {
            av_log(h, AV_LOG_ERROR, "Close %d because new_session failed\n", fd);
            closesocket(fd);
            return ret;
        }

        s->session_id = new_session_id;
    }
    
    av_log(h, AV_LOG_DEBUG, "Opened connection %d\n", fd);

    return fd;
}

static int unix_y_open(URLContext *h, const char *filename, int flags)
{
    UnixYContext *s = h->priv_data;
    int fd;

    s->reading_mode = flags & AVIO_FLAG_READ;
    s->session_id = -1;

    av_strstart(filename, "unix-y:", &filename);
    av_log(h, AV_LOG_DEBUG, "Open file name %s\n", filename);
 
    s->addr.sun_family = AF_UNIX;
    av_strlcpy(s->addr.sun_path, filename, sizeof(s->addr.sun_path));
    if (filename[0] == '0') {
        s->addr.sun_path[0] = '\0';
        s->addr_len = strlen(filename) + offsetof(struct sockaddr_un, sun_path);
        av_log(h, AV_LOG_DEBUG, "Detect abstract domain socket %s\n", &(s->addr.sun_path[1]));
    } else {
        s->addr_len = sizeof(s->addr);
    }

    if (s->timeout < 0)
        s->timeout = 3000;

    if ((fd = connectSocket(h)) < 0) {
        return fd;
    }

    s->control_fd = fd;
    s->cur_fd = -1;
    s->pos = 0;

    h->is_streamed = 0;

    return 0;
}

static int unix_y_read(URLContext *h, uint8_t *buf, int size)
{
    UnixYContext *s = h->priv_data;
    int ret;

    if (!s->reading_mode) {
        av_log(h, AV_LOG_FATAL, "Invalid state: !reading_mode\n");
        return AVERROR(EPERM);
    }

    if (s->cur_fd < 0) {
        av_log(h, AV_LOG_INFO, "cmd: read %" PRId64 "\n", s->pos);
        
        char cmd_buf[50];
        sprintf(cmd_buf, "read %" PRId64 "\n", s->pos);
        if ((ret = write_str(s->control_fd, cmd_buf, strlen(cmd_buf))) < 0) {
            return ret;
        }

        if ((ret = read_str_until(s->control_fd, '\n', cmd_buf, 50)) < 0) {
            return ret;
        }
        if (strcmp(cmd_buf, "ok") != 0) {
            av_log(h, AV_LOG_FATAL, "Not ok %s", cmd_buf);
            return AVERROR(EINVAL);
        }

        ret = connectSocket(h);
        if (ret < 0) {
            return ret;
        } else {
            s->cur_fd = ret;
        }
    }

    ret = recv(s->cur_fd, buf, size, 0);
    if (ret == 0)
        return AVERROR_EOF;
    if (ret < 0) 
        return ff_neterrno();
    s->pos += ret;
    return ret;
}

static int unix_y_write(URLContext *h, const uint8_t *buf, int size)
{
    UnixYContext *s = h->priv_data;
    int ret;

    if (s->reading_mode) {
        av_log(h, AV_LOG_FATAL, "Invalid state: reading_mode\n");
        return AVERROR(EPERM);
    }

    if (s->cur_fd < 0) {
        av_log(h, AV_LOG_INFO, "cmd: write %" PRId64 "\n", s->pos);
        
        char cmd_buf[50];
        sprintf(cmd_buf, "write %" PRId64 "\n", s->pos);
        if ((ret = write_str(s->control_fd, cmd_buf, strlen(cmd_buf))) < 0) {
            return ret;
        }

        if ((ret = read_str_until(s->control_fd, '\n', cmd_buf, 50)) < 0) {
            return ret;
        }
        if (strcmp(cmd_buf, "ok") != 0) {
            av_log(h, AV_LOG_FATAL, "Not ok %s", cmd_buf);
            return AVERROR(EINVAL);
        }

        ret = connectSocket(h);
        if (ret < 0) {
            return ret;
        } else {
            s->cur_fd = ret;
        }
    }

    ret = send(s->cur_fd, buf, size, MSG_NOSIGNAL);
    if (ret < 0) 
        return ff_neterrno();
    s->pos += ret;
    return ret;
}

static int unix_y_close(URLContext *h)
{
    UnixYContext *s = h->priv_data;
    if (s->cur_fd >= 0) {
        av_log(h, AV_LOG_DEBUG, "Close %d\n", s->cur_fd);
        closesocket(s->cur_fd);
    }

    av_log(h, AV_LOG_DEBUG, "Close %d\n", s->control_fd);
    closesocket(s->control_fd);
    return 0;
}

static int64_t unix_y_seek(URLContext *h, int64_t pos, int whence) {
    UnixYContext *s = h->priv_data;
    int ret;
    int64_t stat = -1;

    if ((whence == AVSEEK_SIZE || whence == SEEK_END)) {
        av_log(h, AV_LOG_INFO, "cmd: stat\n");
        
        char cmd_buf[] = "stat\n";
        if ((ret = write_str(s->control_fd, cmd_buf, strlen(cmd_buf))) < 0) {
            return ret;
        }

        av_log(h, AV_LOG_INFO, "reading stat reply\n");
        char rep_buf[50];
        if ((ret = read_str_until(s->control_fd, '\n', rep_buf, sizeof(rep_buf))) < 0) {
            av_log(h, AV_LOG_ERROR, "reading stat reply failure %d %d\n", ret, errno);
            return AVERROR(errno);
        }
        
        av_log(h, AV_LOG_INFO, "rep: stat %s\n", rep_buf);

        stat = strtoll(rep_buf, NULL, 10);

        if (errno == EINVAL) {
            return AVERROR(errno);
        }
    }
    
    if (whence == AVSEEK_SIZE) {
        return stat;
    }

    int64_t newPos = -1;
    if (whence == SEEK_SET) {
        av_log(h, AV_LOG_INFO, "SEEK_SET %" PRId64 " %" PRId64 "\n", s->pos, pos);
        newPos = pos;
    } else if (whence == SEEK_CUR) {
        av_log(h, AV_LOG_INFO, "SEEK_CUR %" PRId64 " %" PRId64 "\n", s->pos, pos);
        newPos = s->pos + pos;
    } else if (whence == SEEK_END) {
        av_log(h, AV_LOG_INFO, "SEEK_END %" PRId64 " %" PRId64 " %" PRId64 "\n", s->pos, pos, stat);
        if (stat < 0) {
            AVERROR(EINVAL);
        }
        newPos = stat + pos;
    } else {
        av_log(h, AV_LOG_FATAL, "Invalid whence %d\n", whence);
        return AVERROR(EINVAL);
    }

    if (s->cur_fd >= 0 && newPos != s->pos) {
        av_log(h, AV_LOG_DEBUG, "Close %d\n", s->cur_fd);
        closesocket(s->cur_fd);
        s->cur_fd = -1;
    }

    s->pos = newPos;

    return newPos;
}

static int unix_y_get_file_handle(URLContext *h)
{
    UnixYContext *s = h->priv_data;
    return s->control_fd;
}

const URLProtocol ff_unix_y_protocol = {
    .name                = "unix-y",
    .url_open            = unix_y_open,
    .url_read            = unix_y_read,
    .url_write           = unix_y_write,
    .url_seek            = unix_y_seek,
    .url_close           = unix_y_close,
    .url_get_file_handle = unix_y_get_file_handle,
    .priv_data_size      = sizeof(UnixYContext),
    .priv_data_class     = &unix_y_class,
    .flags               = URL_PROTOCOL_FLAG_NETWORK
};
