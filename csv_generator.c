#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define INT_PER_LINE 20
/* 13: max length of an integer including '|'
 * 20: total integers per line */
#define MAX_BUF_SIZE 13 * 20

static void write_data(uint64_t lines);
static void *write_data_parallel(void *thread_id);
static int32_t rand_int32();
static uint64_t count_line(const char *filename);

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
        exit(-1);
    }

    char filename[] = "input.csv";
    fp = fopen(filename, "w");

    if (!fp) {
        printf("Error: Cannot open %s\n", filename);
        exit(-1);
    }

    printf("Writing %lu lines of data to %s using %d thread(s)...\n",
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
                exit(-1);
            }
        }
        for (uint32_t i = 0; i < total_threads; i++)
            pthread_join(t[i], NULL);

        free(t);
    }
    fclose(fp);
    end = clock();

    puts("Done.\n");
    printf("Elapsed Time: %.2lf secs\n",
           (double)(end - start) / CLOCKS_PER_SEC);

    /* Checks if there is any data missing */
    uint64_t count = count_line(filename);
    printf("Total lines: %lu\n", count);

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
    printf("thread %d starts.\n", tid);
    uint64_t chuck_size = total_lines / total_threads;
    uint64_t lines = ((tid + 1) * chuck_size > total_lines)
                         ? total_lines % chuck_size
                         : chuck_size;
    write_data(lines);
    printf("thread %d ends.\n", tid);
    pthread_exit(NULL);
}

static int32_t rand_int32() {
    int32_t n = rand();
    int32_t sign = (rand() & 1) << 31;
    return n | sign;
}

static uint64_t count_line(const char *filename) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        printf("Error: Cannot open %s\n", filename);
        exit(-1);
    }

    char *scan_stat;
    char buf[MAX_BUF_SIZE];
    uint64_t count = 0;
    while ((scan_stat = fgets(buf, MAX_BUF_SIZE, fp)) != NULL) {
        uint64_t vertical_count = 0;
        count++;
        for (size_t i = 0; i < strlen(buf); i++) {
            if (buf[i] == '|')
                vertical_count++;
        }
        if (vertical_count != INT_PER_LINE - 1) {
            printf("Error: Wrong format at line %lu\n", count);
            exit(-1);
        }
    }

    fclose(fp);
    return count;
}
