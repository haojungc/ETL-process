#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#define INT_PER_LINE 20
#define BUF_PER_INT 13
#define MAX_BUF_SIZE (INT_PER_LINE * BUF_PER_INT)

static void write_data(uint32_t lines);
static void *write_data_parallel(void *thread_id);
static int32_t rand_int32();

static FILE *fp;
static uint32_t total_lines = 5000000;
static uint32_t total_threads = 1;

int main(int argc, char *argv[]) {
    srand(time(NULL));
    /* Gets the maximum number of threads */
    struct rlimit rlim;
    if (getrlimit(RLIMIT_NPROC, &rlim) == -1) {
        puts("Error: unable to get RLIMIT_NPROC");
        exit(EXIT_FAILURE);
    }

    /* Command Line Arguments */
    switch (argc) {
    case 3:
        /* Invalid format */
        if (sscanf(argv[2], "--thread=%u", &total_threads) != 1) {
            puts("Error: invalid format");
            printf("Usage: %s [--line=<number-of-lines>] "
                   "[--thread=<number-of-threads>] "
                   "(valid range --thread: 1 ~ %lu)\n",
                   argv[0], rlim.rlim_max);
            exit(EXIT_FAILURE);
        }
        /* Invalid value */
        if (!total_threads || total_threads > rlim.rlim_max) {
            puts("Error: invalid value for --thread");
            printf("Usage: %s [--line=<number-of-lines>] "
                   "[--thread=<number-of-threads>] "
                   "(valid range of --thread: 1 ~ %lu)\n",
                   argv[0], rlim.rlim_max);
            exit(EXIT_FAILURE);
        }
    case 2:
        /* Invalid format */
        if (sscanf(argv[1], "--line=%u", &total_lines) != 1) {
            puts("Error: invalid format");
            printf("Usage: %s [--line=<number-of-lines>] "
                   "[--thread=<number-of-threads>]\n",
                   argv[0]);
            exit(EXIT_FAILURE);
        }
        /* Invalid value */
        if (!total_lines || total_lines >> 31 == 1) {
            printf("Error: invalid value for --line, should be between 1 and "
                   "%d\n",
                   INT32_MAX);
            printf("Usage: %s [--line=<number-of-lines>] "
                   "[--thread=<number-of-threads>]\n",
                   argv[0]);
            exit(EXIT_FAILURE);
        }
    case 1:
        break;
    default:
        puts("Error: Too many arguments");
        printf("Usage: %s [--line=<number-of-lines>] "
               "[--thread=<number-of-threads>]\n",
               argv[0]);
        exit(EXIT_FAILURE);
    }

    char filename[] = "input.csv";

    /* Asks for permission to overwrite the existing file */
    if (access(filename, F_OK) == 0) {
        printf("Warning: %s already exists\n", filename);
        puts("Continue? (y: overwrite, n: abort)");
        char c;
        if (scanf("%c", &c) == 1 && (c | ' ') != 'y')
            exit(EXIT_FAILURE);
    }

    if (!(fp = fopen(filename, "w"))) {
        printf("Error: Cannot open %s\n", filename);
        exit(EXIT_FAILURE);
    }

    printf("Writing %u lines of data to %s using %d thread(s) ...\n",
           total_lines, filename, total_threads);

    clock_t start, end;

    start = clock();
    if (total_threads == 1)
        write_data(total_lines);
    else {
        /* Writes data to output file using pthread */
        pthread_t *t = (pthread_t *)malloc(total_threads * sizeof(pthread_t));
        uint32_t *tid = (uint32_t *)malloc(total_threads * sizeof(uint32_t));
        if (!t) {
            perror("malloc failed");
            exit(EXIT_FAILURE);
        }
        for (uint32_t i = 0; i < total_threads; i++) {
            tid[i] = i;
            int32_t stat = pthread_create(&t[i], NULL, write_data_parallel,
                                          (void *)&tid[i]);
            if (stat) {
                printf("ERROR: return code from pthread_create() is %d\n",
                       stat);
                exit(EXIT_FAILURE);
            }
        }
        for (uint32_t i = 0; i < total_threads; i++)
            pthread_join(t[i], NULL);

        free(t);
        free(tid);
    }
    fclose(fp);
    end = clock();

    puts("Done.");
    printf("Elapsed Time: %.2lf secs\n",
           (double)(end - start) / CLOCKS_PER_SEC);

    return 0;
}

static void write_data(uint32_t lines) {
    int32_t n[INT_PER_LINE];
    for (uint32_t i = 0; i < lines; i++) {
        for (uint32_t j = 0; j < INT_PER_LINE; j++) {
            n[j] = rand_int32();
        }
        fprintf(
            fp, "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d\n",
            n[0], n[1], n[2], n[3], n[4], n[5], n[6], n[7], n[8], n[9], n[10],
            n[11], n[12], n[13], n[14], n[15], n[16], n[17], n[18], n[19]);
    }
}

static void *write_data_parallel(void *thread_id) {
    uint32_t tid = *(uint32_t *)thread_id;
    printf("  Starting thread %d ...\n", tid);
    uint32_t chuck_size = total_lines / total_threads;
    uint32_t lines = ((tid + 1) * chuck_size > total_lines)
                         ? total_lines % chuck_size
                         : chuck_size;
    write_data(lines);
    printf("  Ending thread %d ...\n", tid);
    pthread_exit(NULL);
}

static int32_t rand_int32() {
    int32_t n = rand();
    int32_t sign = (rand() & 1) << 31;
    return n | sign;
}
