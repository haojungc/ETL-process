#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>

#define MIN(x, y) ((x) < (y) ? (x) : (y))

#define MAX_LEN 150000000
#define CHUNK_SIZE 1000000
#define INT_PER_LINE 20
#define BUF_PER_INT 25
#define BUF_SIZE (CHUNK_SIZE * INT_PER_LINE * BUF_PER_INT)

typedef struct {
    uint32_t chunk_index;
    uint32_t start_index;
    uint32_t total_lines;
} task_t;

static void convert_csv_to_json(const char *f_out, const char *f_in,
                                const uint32_t total_threads);
static void write_data(const uint64_t total_lines);
static void write_data_parallel(const uint64_t total_lines,
                                const uint32_t total_threads);
static void *convert_line(void *task);

static FILE *fp_in, *fp_out;
int32_t n[MAX_LEN];
char buf[BUF_SIZE];

int main(int argc, char *argv[]) {
    /* Gets the maximum number of threads */
    struct rlimit rlim;
    if (getrlimit(RLIMIT_NPROC, &rlim) == -1) {
        puts("Error: unable to get RLIMIT_NPROC");
        exit(EXIT_FAILURE);
    }

    clock_t start, end;
    const char input_filename[] = "input.csv";
    const char output_filename[] = "output.json";
    static uint32_t total_threads = 1;

    start = clock();

    /* Too many arguments */
    if (argc > 2) {
        puts("Error: too many arguments");
        printf(
            "Usage: %s [--thread=<number-of-threads>] (valid range: 1 ~ %lu)\n",
            argv[0], rlim.rlim_max);
        exit(EXIT_FAILURE);
    }
    /* Sets the number of threads */
    if (argc == 2) {
        /* Invalid format */
        if (sscanf(argv[1], "--thread=%u", &total_threads) != 1) {
            puts("Error: invalid format");
            printf("Usage: %s [--thread=<number-of-threads>] "
                   "(valid range: 1 ~ %lu)\n",
                   argv[0], rlim.rlim_max);
            exit(EXIT_FAILURE);
        }
        /* Invalid value */
        if (!total_threads || total_threads > rlim.rlim_max) {
            puts("Error: invalid value for --thread");
            printf("Usage: %s [--thread=<number-of-threads>] "
                   "(valid range: 1 ~ %lu)\n",
                   argv[0], rlim.rlim_max);
            exit(EXIT_FAILURE);
        }
    }

    convert_csv_to_json(output_filename, input_filename, total_threads);

    end = clock();

    printf("Elapsed Time: %.2f secs\n", (double)(end - start) / CLOCKS_PER_SEC);

    return 0;
}

static void convert_csv_to_json(const char *f_out, const char *f_in,
                                const uint32_t total_threads) {
    /* Opens the input file */
    if (!(fp_in = fopen(f_in, "r"))) {
        printf("Error: unable to open %s\n", f_in);
        exit(EXIT_FAILURE);
    }

    /* Checks the size of input file */
    struct stat st;
    fstat(fileno(fp_in), &st);
    off_t filesize = st.st_size;
    printf("Size of %s: %.2f MB\n", f_in, (double)filesize / 1e6);

    /* Opens the output file */
    if (!(fp_out = fopen(f_out, "w"))) {
        printf("Error: unable to open %s\n", f_out);
        exit(EXIT_FAILURE);
    }

    clock_t start, end;
    printf("Converting CSV to JSON with %u thread(s) ...\n", total_threads);

    /* Reads from input file */
    printf("  Reading from %s ...\n", f_in);

    start = clock();
    uint64_t total_lines = 0;
    uint32_t offset = 0;
    while (fscanf(fp_in,
                  "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d",
                  &n[offset], &n[offset + 1], &n[offset + 2], &n[offset + 3],
                  &n[offset + 4], &n[offset + 5], &n[offset + 6],
                  &n[offset + 7], &n[offset + 8], &n[offset + 9],
                  &n[offset + 10], &n[offset + 11], &n[offset + 12],
                  &n[offset + 13], &n[offset + 14], &n[offset + 15],
                  &n[offset + 16], &n[offset + 17], &n[offset + 18],
                  &n[offset + 19]) == INT_PER_LINE) {
        total_lines++;
        offset += INT_PER_LINE;
    }
    fclose(fp_in);
    end = clock();
    printf("  Execution Time: %.2f secs\n",
           (double)(end - start) / CLOCKS_PER_SEC);

    /* Writes to output file */
    printf("  Writing data to %s ...\n", f_out);
    start = clock();
    fprintf(fp_out, "[");
    if (total_threads == 1) {
        write_data(total_lines);
    } else {
        write_data_parallel(total_lines, total_threads);
    }
    fprintf(fp_out, "\n]");
    fclose(fp_out);
    end = clock();
    printf("  Execution Time: %.2f secs\n",
           (double)(end - start) / CLOCKS_PER_SEC);
}

static void write_data(const uint64_t total_lines) {
    static const char format[] = "%s\n"
                                 "\t{\n"
                                 "\t\t\"col_1\":%d,\n"
                                 "\t\t\"col_2\":%d,\n"
                                 "\t\t\"col_3\":%d,\n"
                                 "\t\t\"col_4\":%d,\n"
                                 "\t\t\"col_5\":%d,\n"
                                 "\t\t\"col_6\":%d,\n"
                                 "\t\t\"col_7\":%d,\n"
                                 "\t\t\"col_8\":%d,\n"
                                 "\t\t\"col_9\":%d,\n"
                                 "\t\t\"col_10\":%d,\n"
                                 "\t\t\"col_11\":%d,\n"
                                 "\t\t\"col_12\":%d,\n"
                                 "\t\t\"col_13\":%d,\n"
                                 "\t\t\"col_14\":%d,\n"
                                 "\t\t\"col_15\":%d,\n"
                                 "\t\t\"col_16\":%d,\n"
                                 "\t\t\"col_17\":%d,\n"
                                 "\t\t\"col_18\":%d,\n"
                                 "\t\t\"col_19\":%d,\n"
                                 "\t\t\"col_20\":%d\n"
                                 "\t}";
    uint32_t offset = 0;
    for (uint32_t i = 0; i < total_lines; i++) {
        fprintf(fp_out, format, i == 0 ? "" : ",", n[offset], n[offset + 1],
                n[offset + 2], n[offset + 3], n[offset + 4], n[offset + 5],
                n[offset + 6], n[offset + 7], n[offset + 8], n[offset + 9],
                n[offset + 10], n[offset + 11], n[offset + 12], n[offset + 13],
                n[offset + 14], n[offset + 15], n[offset + 16], n[offset + 17],
                n[offset + 18], n[offset + 19]);
        offset += INT_PER_LINE;
    }
}

static void write_data_parallel(const uint64_t _total_lines,
                                const uint32_t total_threads) {
    uint64_t total_lines = _total_lines;
    pthread_t *t = malloc(total_threads * sizeof(pthread_t));
    task_t *task = malloc(total_threads * sizeof(task_t));

    if (!t || !task) {
        perror("malloc failed");
        exit(EXIT_FAILURE);
    }

    uint32_t chunk_index = 0;
    while (total_lines > 0) {
        uint32_t current_chunk_size = MIN(total_lines, CHUNK_SIZE);
        uint32_t section_size = current_chunk_size / total_threads;
        uint32_t last_section_size =
            current_chunk_size - section_size * (total_threads - 1);
        for (uint32_t i = 0; i < total_threads; i++) {
            task[i].chunk_index = chunk_index;
            task[i].start_index = i * section_size;
            task[i].total_lines =
                (i == total_threads - 1) ? last_section_size : section_size;
            pthread_create(&t[i], NULL, convert_line, (void *)&task[i]);
        }

        for (uint32_t i = 0; i < total_threads; i++)
            pthread_join(t[i], NULL);

        /* Prints buf to output file */
        for (uint32_t i = 0; i < total_threads; i++) {
            fputs(&buf[task[i].start_index * INT_PER_LINE * BUF_PER_INT],
                  fp_out);
        }
        total_lines -= current_chunk_size;
        chunk_index++;
    }

    free(t);
    free(task);
}

static void *convert_line(void *_task) {
    task_t *task = (task_t *)_task;

    static const char format[] = "%s\n"
                                 "\t{\n"
                                 "\t\t\"col_1\":%d,\n"
                                 "\t\t\"col_2\":%d,\n"
                                 "\t\t\"col_3\":%d,\n"
                                 "\t\t\"col_4\":%d,\n"
                                 "\t\t\"col_5\":%d,\n"
                                 "\t\t\"col_6\":%d,\n"
                                 "\t\t\"col_7\":%d,\n"
                                 "\t\t\"col_8\":%d,\n"
                                 "\t\t\"col_9\":%d,\n"
                                 "\t\t\"col_10\":%d,\n"
                                 "\t\t\"col_11\":%d,\n"
                                 "\t\t\"col_12\":%d,\n"
                                 "\t\t\"col_13\":%d,\n"
                                 "\t\t\"col_14\":%d,\n"
                                 "\t\t\"col_15\":%d,\n"
                                 "\t\t\"col_16\":%d,\n"
                                 "\t\t\"col_17\":%d,\n"
                                 "\t\t\"col_18\":%d,\n"
                                 "\t\t\"col_19\":%d,\n"
                                 "\t\t\"col_20\":%d\n"
                                 "\t}";
    uint32_t buf_index = task->start_index * INT_PER_LINE * BUF_PER_INT;
    uint32_t offset = task->chunk_index * CHUNK_SIZE * INT_PER_LINE +
                      task->start_index * INT_PER_LINE;
    int32_t len;
    for (uint32_t i = 0; i < task->total_lines; i++) {
        len = snprintf(
            &buf[buf_index], INT_PER_LINE * BUF_PER_INT * sizeof(char), format,
            (task->chunk_index == 0 && buf_index == 0) ? "" : ",", n[offset],
            n[offset + 1], n[offset + 2], n[offset + 3], n[offset + 4],
            n[offset + 5], n[offset + 6], n[offset + 7], n[offset + 8],
            n[offset + 9], n[offset + 10], n[offset + 11], n[offset + 12],
            n[offset + 13], n[offset + 14], n[offset + 15], n[offset + 16],
            n[offset + 17], n[offset + 18], n[offset + 19]);
        buf_index += len;
        offset += INT_PER_LINE;
    }

    pthread_exit(NULL);
}
