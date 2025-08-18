#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> /* unlink() */
#include <sys/types.h> /* open() */
#include <sys/stat.h> /* open() */
#include <fcntl.h> /* open() */
#include <errno.h>

#include <mpi.h>

#define CHECK_ERROR(fnc) { \
    if (err != MPI_SUCCESS) { \
        int errorStringLen; \
        char errorString[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(err, errorString, &errorStringLen); \
        printf("Error at line %d when calling %s: %s\n",__LINE__,fnc,errorString); \
        err = -1; \
        goto err_out; \
    } \
}

#define NELEMS 1048576

#define CHECK_EXP(expect, ptr, offset, length) { \
    int k; \
    char *ptr = buf + offset; \
    for (k=0; k<length; k++) { \
        if (ptr[k] != expect) { \
            printf("Error at line %d: off=%d expect %c but got %c\n", __LINE__, offset+k, expect, ptr[k]); \
            err = -1; \
            goto err_out; \
        } \
    } \
}

#ifndef MAX
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#endif

/*----< main() >------------------------------------------------------------*/
int main(int argc, char **argv)
{
    char buf[NELEMS], *filename="testfie";
    int i, j, err=0, rank, np, omode;
    MPI_Offset offset;
    MPI_Count nbytes;
    MPI_File fh;
    MPI_Status st;
    MPI_Info info=MPI_INFO_NULL;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &np);

    MPI_Info_create(&info);
    MPI_Info_set(info, "cb_nodes", "4");
    MPI_Info_set(info, "romio_no_indep_rw", "true");

    int record_off, record_len, header_off, header_len;
    int offsets[4][4], lengths[4][4];

/*
    record_off = 1328; record_len = 515;
    header_off = 0;    header_len = 1328;
    offsets[0][0] =1843; lengths[0][0] =194;
    offsets[0][1] =2412; lengths[0][1] =301;
    offsets[0][2] =3025; lengths[0][2] =107;
    offsets[0][3] =3132; lengths[0][3] =63;
    offsets[1][0] =2037; lengths[1][0] =125;
    offsets[1][1] =2713; lengths[1][1] =104;
    offsets[1][2] =3132; lengths[1][2] =0;
    offsets[1][3] =3195; lengths[1][3] =61;
    offsets[2][0] =2162; lengths[2][0] =125;
    offsets[2][1] =2817; lengths[2][1] =102;
    offsets[2][2] =3132; lengths[2][2] =0;
    offsets[2][3] =3256; lengths[2][3] =63;
    offsets[3][0] =2287; lengths[3][0] =125;
    offsets[3][1] =2919; lengths[3][1] =106;
    offsets[3][2] =3132; lengths[3][2] =0;
    offsets[3][3] =3319; lengths[3][3] =63;
*/

    record_off = 1328; record_len = 246;
    header_off = 0;    header_len = 1328;

    offsets[0][0] =1574; lengths[0][0] =218;
    offsets[0][1] =2235; lengths[0][1] =303;
    offsets[0][2] =2849; lengths[0][2] =106;
    offsets[0][3] =2955; lengths[0][3] =62;

    offsets[1][0] =1792; lengths[1][0] =146;
    offsets[1][1] =2538; lengths[1][1] =104;
    offsets[1][2] =2955; lengths[1][2] =0;
    offsets[1][3] =3017; lengths[1][3] =60;

    offsets[2][0] =1938; lengths[2][0] =148;
    offsets[2][1] =2642; lengths[2][1] =105;
    offsets[2][2] =2955; lengths[2][2] =0;
    offsets[2][3] =3077; lengths[2][3] =62;

    offsets[3][0] =2086; lengths[3][0] =149;
    offsets[3][1] =2747; lengths[3][1] =102;
    offsets[3][2] =2955; lengths[3][2] =0;
    offsets[3][3] =3139; lengths[3][3] =61;

    size_t file_size = record_off + record_len;
    for (i=0; i<4; i++)
        for (j=0; j<4; j++)
            file_size = MAX(file_size,  offsets[i][j] + lengths[i][j]);

    if (rank == 0) {
        unlink(filename);
        printf("file_size=%zd\n",file_size);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    omode = MPI_MODE_CREATE | MPI_MODE_RDWR;

    err = MPI_File_open(MPI_COMM_WORLD, filename, omode, info, &fh);
    CHECK_ERROR("MPI_File_open")

    if (rank == 0) {
        offset = record_off;
        nbytes = record_len;
        for (j=0; j<nbytes; j++) buf[j] = (offset+j) % 128;
        MPI_File_write_at(fh, offset, buf, nbytes, MPI_BYTE, &st);
    }

    for (i=0; i<4; i++) {
        offset = offsets[rank][i];
        nbytes = lengths[rank][i];
        for (j=0; j<nbytes; j++) buf[j] = (offset+j) % 128;
        MPI_File_write_at_all(fh, offset, buf, nbytes, MPI_BYTE, &st);
    }

    if (rank == 0) {
        offset = header_off;
        nbytes = header_len;
        for (j=0; j<nbytes; j++) buf[j] = (offset+j) % 128;
        MPI_File_write_at(fh, offset, buf, nbytes, MPI_BYTE, &st);
    }
    err = MPI_File_close(&fh);
    CHECK_ERROR("MPI_File_close")

    /* reset buffer contents */
    for (i=0; i<file_size; i++) buf[i] = -1;

    /* read back and check contents */
    if (rank == 0) {
        int fd = open(filename, O_RDONLY, 0600);
        if (fd < 0) {
            printf("Error at line %d : opening file %s (%s)\n", __LINE__, filename, strerror(errno));
            err = -1;
            goto err_out;
        }

        /* read the whole file */
        size_t rlen = read(fd, buf, file_size);
        close(fd);

        /* check contents */
        for (i=0; i<file_size; i++) {
            char expect = i % 128;
            if (buf[i] != expect) {
                printf("Error at line %d: off=%d expect %d but got %d\n", __LINE__, i, expect, buf[i]);
                err = -1;
                // goto err_out;
            }
        }
    }

err_out:
    MPI_Info_free(&info);
    MPI_Finalize();
    return (err != 0);
}


