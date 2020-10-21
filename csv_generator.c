#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define INT_PER_LINE 20
#define BUF_PER_INT 13
#define MAX_BUF_SIZE (INT_PER_LINE * BUF_PER_INT)

static void write_data(uint64_t lines);
static void *write_data_parallel(void *thread_id);
static int32_t rand_int32();

static FILE *fp;
static uint64_t total_lines = 2e7;
static uint32_t total_threads = 1;

int main(int argc, char *argv[]) {
    srand(time(NULL));

    /* Command Line Arguments */
    switch (argc) {
    case 3:
        total_lines = atoll(argv[2]);
    case 2:
        total_threads = atoi(argv[1]);
        break;
    case 1:
        break;
    default:
        puts("Error: Too many arguments");
        printf("Usage: %s [threads] [total-lines-of-data]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char filename[] = "input.csv";

    /* Asks for permission to overwrite the existing file */
    if (access(filename, F_OK) == 0) {
        printf("Warning: %s already exists\n", filename);
        puts("Continue? (y: overwrite, n: abort)");
        char c;
        scanf("%c", &c);
        if ((c | ' ') != 'y')
            exit(EXIT_FAILURE);
    }

    if (!(fp = fopen(filename, "w"))) {
        printf("Error: Cannot open %s\n", filename);
        exit(EXIT_FAILURE);
    }

    printf("Writing %lu lines of data to %s using %d thread(s) ...\n",
           total_lines, filename, total_threads);

    clock_t start, end;

    start = clock();
    if (total_threads == 1)
        write_data(total_lines);
    else {
        /* Writes data to output file using pthread */
        pthread_t *t = malloc(total_threads * sizeof(pthread_t));
        for (uint32_t i = 0; i < total_threads; i++) {
            int32_t stat =
                pthread_create(&t[i], NULL, write_data_parallel, (void *)i);
            if (stat) {
                printf("ERROR: return code from pthread_create() is %d\n",
                       stat);
                exit(EXIT_FAILURE);
            }
        }
        for (uint32_t i = 0; i < total_threads; i++)
            pthread_join(t[i], NULL);

        free(t);
    }
    fclose(fp);
    end = clock();

    puts("Done.");
    printf("-----\nElapsed Time: %.2lf secs\n-----\n",
           (double)(end - start) / CLOCKS_PER_SEC);

    return 0;
}

static void write_data(uint64_t lines) {
    int32_t n[INT_PER_LINE];
    for (uint64_t i = 0; i < lines; i++) {
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
    uint32_t tid = (uint32_t)thread_id;
    printf("  Starting thread %d ...\n", tid);
    uint64_t chuck_size = total_lines / total_threads;
    uint64_t lines = ((tid + 1) * chuck_size > total_lines)
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
