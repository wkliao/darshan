/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#define _GNU_SOURCE
#include "darshan-util-config.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "darshan-logutils.h"

/* integer counter name strings for the MDHIM module */
#define X(a) #a,
char *mdhim_counter_names[] = {
    MDHIM_COUNTERS
};

/* floating point counter name strings for the MDHIM module */
char *mdhim_f_counter_names[] = {
    MDHIM_F_COUNTERS
};
#undef X

/* prototypes for each of the MDHIM module's logutil functions */
static int darshan_log_get_mdhim_record(darshan_fd fd, void** mdhim_buf_p);
static int darshan_log_put_mdhim_record(darshan_fd fd, void* mdhim_buf);
static void darshan_log_print_mdhim_record(void *file_rec,
    char *file_name, char *mnt_pt, char *fs_type);
static void darshan_log_print_mdhim_description(int ver);
static void darshan_log_print_mdhim_record_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2);
static void darshan_log_agg_mdhim_records(void *rec, void *agg_rec, int init_flag);

/* structure storing each function needed for implementing the darshan
 * logutil interface. these functions are used for reading, writing, and
 * printing module data in a consistent manner.
 */
struct darshan_mod_logutil_funcs mdhim_logutils =
{
    .log_get_record = &darshan_log_get_mdhim_record,
    .log_put_record = &darshan_log_put_mdhim_record,
    .log_print_record = &darshan_log_print_mdhim_record,
    .log_print_description = &darshan_log_print_mdhim_description,
    .log_print_diff = &darshan_log_print_mdhim_record_diff,
    .log_agg_records = &darshan_log_agg_mdhim_records
};

/* retrieve a MDHIM record from log file descriptor 'fd', storing the
 * data in the buffer address pointed to by 'mdhim_buf_p'. Return 1 on
 * successful record read, 0 on no more data, and -1 on error.
 */
static int darshan_log_get_mdhim_record(darshan_fd fd, void** mdhim_buf_p)
{
    struct darshan_mdhim_record *rec =
        *((struct darshan_mdhim_record **)mdhim_buf_p);
    int i;
    int ret;

    if(fd->mod_map[DARSHAN_MDHIM_MOD].len == 0)
        return(0);

    if(*mdhim_buf_p == NULL)
    {
        rec = malloc(sizeof(*rec));
        if(!rec)
            return(-1);
    }

    /* read a MDHIM module record from the darshan log file */
    ret = darshan_log_get_mod(fd, DARSHAN_MDHIM_MOD, rec,
        sizeof(struct darshan_mdhim_record));

    if(*mdhim_buf_p == NULL)
    {
        if(ret == sizeof(struct darshan_mdhim_record))
            *mdhim_buf_p = rec;
        else
            free(rec);
    }

    if(ret < 0)
        return(-1);
    else if(ret < sizeof(struct darshan_mdhim_record))
        return(0);
    else
    {
        /* if the read was successful, do any necessary byte-swapping */
        if(fd->swap_flag)
        {
            DARSHAN_BSWAP64(&(rec->base_rec.id));
            DARSHAN_BSWAP64(&(rec->base_rec.rank));
            for(i=0; i<MDHIM_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->counters[i]);
            for(i=0; i<MDHIM_F_NUM_INDICES; i++)
                DARSHAN_BSWAP64(&rec->fcounters[i]);
        }

        return(1);
    }
}

/* write the MDHIM record stored in 'mdhim_buf' to log file descriptor 'fd'.
 * Return 0 on success, -1 on failure
 */
static int darshan_log_put_mdhim_record(darshan_fd fd, void* mdhim_buf)
{
    struct darshan_mdhim_record *rec = (struct darshan_mdhim_record *)mdhim_buf;
    int ret;

    /* append MDHIM record to darshan log file */
    ret = darshan_log_put_mod(fd, DARSHAN_MDHIM_MOD, rec,
        sizeof(struct darshan_mdhim_record), DARSHAN_MDHIM_VER);
    if(ret < 0)
        return(-1);

    return(0);
}

/* print all I/O data record statistics for the given MDHIM record */
static void darshan_log_print_mdhim_record(void *file_rec, char *file_name,
    char *mnt_pt, char *fs_type)
{
    int i;
    struct darshan_mdhim_record *mdhim_rec =
        (struct darshan_mdhim_record *)file_rec;

    /* print each of the integer and floating point counters for the MDHIM module */
    for(i=0; i<MDHIM_NUM_INDICES; i++)
    {
        /* macro defined in darshan-logutils.h */
        DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
            mdhim_rec->base_rec.rank, mdhim_rec->base_rec.id,
            mdhim_counter_names[i], mdhim_rec->counters[i],
            file_name, mnt_pt, fs_type);
    }

    for(i=0; i<MDHIM_F_NUM_INDICES; i++)
    {
        /* macro defined in darshan-logutils.h */
        DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
            mdhim_rec->base_rec.rank, mdhim_rec->base_rec.id,
            mdhim_f_counter_names[i], mdhim_rec->fcounters[i],
            file_name, mnt_pt, fs_type);
    }

    return;
}

/* print out a description of the MDHIM module record fields */
static void darshan_log_print_mdhim_description(int ver)
{
    printf("\n# description of MDHIM counters:\n");
    printf("#   MDHIM_PUTS: number of 'mdhim_put' function calls.\n");
    printf("#   MDHIM_GETS: number of 'mdhim_get' function calls.\n");
    printf("#   MDHIM_F_PUT_TIMESTAMP: timestamp of the first call to function 'mdhim_put'.\n");
    printf("#   MDHIM_F_GET_TIMESTAMP: timestamp of the first call to function 'mdhim_get'.\n");

    return;
}

/* print a diff of two MDHIM records (with the same record id) */
static void darshan_log_print_mdhim_record_diff(void *file_rec1, char *file_name1,
    void *file_rec2, char *file_name2)
{
    struct darshan_mdhim_record *file1 = (struct darshan_mdhim_record *)file_rec1;
    struct darshan_mdhim_record *file2 = (struct darshan_mdhim_record *)file_rec2;
    int i;

    /* NOTE: we assume that both input records are the same module format version */

    for(i=0; i<MDHIM_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
                file1->base_rec.rank, file1->base_rec.id, mdhim_counter_names[i],
                file1->counters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
                file2->base_rec.rank, file2->base_rec.id, mdhim_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
        else if(file1->counters[i] != file2->counters[i])
        {
            printf("- ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
                file1->base_rec.rank, file1->base_rec.id, mdhim_counter_names[i],
                file1->counters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
                file2->base_rec.rank, file2->base_rec.id, mdhim_counter_names[i],
                file2->counters[i], file_name2, "", "");
        }
    }

    for(i=0; i<MDHIM_F_NUM_INDICES; i++)
    {
        if(!file2)
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
                file1->base_rec.rank, file1->base_rec.id, mdhim_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");

        }
        else if(!file1)
        {
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
                file2->base_rec.rank, file2->base_rec.id, mdhim_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
        else if(file1->fcounters[i] != file2->fcounters[i])
        {
            printf("- ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
                file1->base_rec.rank, file1->base_rec.id, mdhim_f_counter_names[i],
                file1->fcounters[i], file_name1, "", "");
            printf("+ ");
            DARSHAN_F_COUNTER_PRINT(darshan_module_names[DARSHAN_MDHIM_MOD],
                file2->base_rec.rank, file2->base_rec.id, mdhim_f_counter_names[i],
                file2->fcounters[i], file_name2, "", "");
        }
    }

    return;
}

/* aggregate the input MDHIM record 'rec'  into the output record 'agg_rec' */
static void darshan_log_agg_mdhim_records(void *rec, void *agg_rec, int init_flag)
{
    struct darshan_mdhim_record *mdhim_rec = (struct darshan_mdhim_record *)rec;
    struct darshan_mdhim_record *agg_mdhim_rec = (struct darshan_mdhim_record *)agg_rec;
    int i;

    for(i = 0; i < MDHIM_NUM_INDICES; i++)
    {
        switch(i)
        {
            case MDHIM_PUTS:
                /* sum */
                agg_mdhim_rec->counters[i] += mdhim_rec->counters[i];
                break;
            case MDHIM_GETS:
                /* sum */
                agg_mdhim_rec->counters[i] += mdhim_rec->counters[i];
                break;
            default:
                /* if we don't know how to aggregate this counter, just set to -1 */
                agg_mdhim_rec->counters[i] = -1;
                break;
        }
    }

    for(i = 0; i < MDHIM_F_NUM_INDICES; i++)
    {
        switch(i)
        {
            case MDHIM_F_PUT_TIMESTAMP:
                /* min non-zero */
                if((mdhim_rec->fcounters[i] > 0)  &&
                    ((agg_mdhim_rec->fcounters[i] == 0) ||
                    (mdhim_rec->fcounters[i] < agg_mdhim_rec->fcounters[i])))
                {
                    agg_mdhim_rec->fcounters[i] = mdhim_rec->fcounters[i];
                }
                break;
            case MDHIM_F_GET_TIMESTAMP:
                /* min non-zero */
                if((mdhim_rec->fcounters[i] > 0)  &&
                    ((agg_mdhim_rec->fcounters[i] == 0) ||
                    (mdhim_rec->fcounters[i] < agg_mdhim_rec->fcounters[i])))
                {
                    agg_mdhim_rec->fcounters[i] = mdhim_rec->fcounters[i];
                }
                break;

            default:
                /* if we don't know how to aggregate this counter, just set to -1 */
                agg_mdhim_rec->fcounters[i] = -1;
                break;
        }
    }

    return;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */