#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>

#define MIN(x, y) ((x) < (y) ? (x) : (y))

#define CHUNK_SIZE 2500000
#define INT_PER_LINE 20
#define BUF_PER_INT 25
#define BUF_PER_LINE (INT_PER_LINE * BUF_PER_INT)
#define MIN_BUF_PER_LINE 40 /* 20 zeros, 19 columns and 1 newline character */
#define BUF_SIZE (CHUNK_SIZE * BUF_PER_LINE)
#define JSON_FORMAT                                                            \
    "%s\n"                                                                     \
    "\t{\n"                                                                    \
    "\t\t\"col_1\":%s,\n"                                                      \
    "\t\t\"col_2\":%s,\n"                                                      \
    "\t\t\"col_3\":%s,\n"                                                      \
    "\t\t\"col_4\":%s,\n"                                                      \
    "\t\t\"col_5\":%s,\n"                                                      \
    "\t\t\"col_6\":%s,\n"                                                      \
    "\t\t\"col_7\":%s,\n"                                                      \
    "\t\t\"col_8\":%s,\n"                                                      \
    "\t\t\"col_9\":%s,\n"                                                      \
    "\t\t\"col_10\":%s,\n"                                                     \
    "\t\t\"col_11\":%s,\n"                                                     \
    "\t\t\"col_12\":%s,\n"                                                     \
    "\t\t\"col_13\":%s,\n"                                                     \
    "\t\t\"col_14\":%s,\n"                                                     \
    "\t\t\"col_15\":%s,\n"                                                     \
    "\t\t\"col_16\":%s,\n"                                                     \
    "\t\t\"col_17\":%s,\n"                                                     \
    "\t\t\"col_18\":%s,\n"                                                     \
    "\t\t\"col_19\":%s,\n"                                                     \
    "\t\t\"col_20\":%s\n"                                                      \
    "\t}"

typedef struct {
    uint32_t chunk_index;
    uint32_t start_index;
    uint32_t total_lines;
    size_t length;
} task_t;

static void convert_csv_to_json(const char *f_out, const char *f_in,
                                const uint32_t total_threads);
static uint64_t read_data(const char *f_in, const uint32_t total_threads);
static uint64_t set_pos(const size_t size, const uint32_t total_threads);
static void write_data(const uint64_t total_lines);
static void write_data_parallel(const uint64_t total_lines,
                                const uint32_t total_threads);
static void *convert_line(void *task);

static FILE *fp_in, *fp_out;
static uint32_t *pos; /* starting positions of each integer in buf_in */
static char *buf_in;
static char buf_out[BUF_SIZE];

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
    uint32_t total_threads = 1;

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
    start = clock();
    convert_csv_to_json(output_filename, input_filename, total_threads);
    end = clock();

    printf("Total CPU time used: %.2f s\n",
           (double)(end - start) / CLOCKS_PER_SEC);

    return 0;
}

static void convert_csv_to_json(const char *f_out, const char *f_in,
                                const uint32_t total_threads) {
    printf("Converting CSV to JSON with %u thread(s) ...\n", total_threads);
    clock_t start, end;

    /* Reads from input file */
    start = clock();
    uint64_t total_lines = read_data(f_in, total_threads);
    end = clock();

    printf("  CPU time used: %.2f s, done.\n",
           (double)(end - start) / CLOCKS_PER_SEC);

    /* Writes to output file */
    printf("  Writing %lu lines of data to %s ...\n", total_lines, f_out);

    /* Opens the output file */
    if (!(fp_out = fopen(f_out, "wb"))) {
        printf("Error: unable to open %s\n", f_out);
        exit(EXIT_FAILURE);
    }

    /* Writes data to output file */
    start = clock();
    fwrite("[", sizeof(char), 1, fp_out);
    if (total_threads == 1) {
        write_data(total_lines);
    } else {
        write_data_parallel(total_lines, total_threads);
    }
    fwrite("\n]", sizeof(char), 2, fp_out);
    fclose(fp_out);
    free(buf_in);
    free(pos);
    end = clock();

    printf("  CPU time used: %.2f s, done.\n",
           (double)(end - start) / CLOCKS_PER_SEC);
}

static uint64_t read_data(const char *f_in, const uint32_t total_threads) {
    /* Opens input file */
    if (!(fp_in = fopen(f_in, "rb"))) {
        printf("Error: unable to open %s\n", f_in);
        exit(EXIT_FAILURE);
    }

    /* Checks the size of input file */
    struct stat st;
    fstat(fileno(fp_in), &st);
    off_t filesize = st.st_size;
    printf("  Reading from %s (size: %.2f MB) ...\n", f_in,
           (double)filesize / 1e6);

    /* Allocates memory for buf_in */
    buf_in = (char *)malloc((filesize + 1) * sizeof(char));
    if (!buf_in) {
        perror("malloc failed");
        exit(EXIT_FAILURE);
    }

    /* Reads data from input file */
    size_t result = fread(buf_in, sizeof(char), filesize, fp_in);
    if (result != filesize) {
        printf("error: fread failed\n");
        exit(EXIT_FAILURE);
    }
    fclose(fp_in);

    pos = (uint32_t *)malloc((filesize / MIN_BUF_PER_LINE * INT_PER_LINE) *
                             sizeof(uint32_t));
    if (!pos) {
        perror("malloc failed");
        exit(EXIT_FAILURE);
    }

    uint64_t total_lines = set_pos(filesize, total_threads);

    return total_lines;
}

static uint64_t set_pos(const size_t size, const uint32_t total_threads) {
    uint64_t count = 0;
    uint64_t line_count = 1;

    /* Stores the starting positions of each integer
     * and ends each of them with '\0' */
    pos[count++] = 0;
    for (uint64_t i = 2; i < size; i++) {
        switch (buf_in[i - 1]) {
        case '\n':
            line_count++;
        case '|':
            pos[count++] = i;
            buf_in[i - 1] = '\0';
        }
    }
    buf_in[size - 1] = '\0';

    printf("  Total Lines: %lu\n"
           "  Total Integers: %lu\n",
           line_count, count);
    return line_count;
}

static void write_data(const uint64_t total_lines) {
    static const char format[] = JSON_FORMAT;
    char str[BUF_PER_LINE];
    uint32_t offset = 0;
    size_t len;

    /* Writes data to output file in JSON format */
    for (uint32_t i = 0; i < total_lines; i++) {
        len = snprintf(str, BUF_PER_LINE, format, i == 0 ? "" : ",",
                       &buf_in[pos[offset]], &buf_in[pos[offset + 1]],
                       &buf_in[pos[offset + 2]], &buf_in[pos[offset + 3]],
                       &buf_in[pos[offset + 4]], &buf_in[pos[offset + 5]],
                       &buf_in[pos[offset + 6]], &buf_in[pos[offset + 7]],
                       &buf_in[pos[offset + 8]], &buf_in[pos[offset + 9]],
                       &buf_in[pos[offset + 10]], &buf_in[pos[offset + 11]],
                       &buf_in[pos[offset + 12]], &buf_in[pos[offset + 13]],
                       &buf_in[pos[offset + 14]], &buf_in[pos[offset + 15]],
                       &buf_in[pos[offset + 16]], &buf_in[pos[offset + 17]],
                       &buf_in[pos[offset + 18]], &buf_in[pos[offset + 19]]);
        fwrite(str, sizeof(char), len, fp_out);
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
            uint32_t start_index =
                task[i].start_index * INT_PER_LINE * BUF_PER_INT;
            fwrite(&buf_out[start_index], sizeof(char), task[i].length, fp_out);
        }
        total_lines -= current_chunk_size;
        chunk_index++;
    }
    free(t);
    free(task);
}

static void *convert_line(void *_task) {
    static const char format[] = JSON_FORMAT;
    task_t *task = (task_t *)_task;
    uint32_t start_index = task->start_index * INT_PER_LINE * BUF_PER_INT;

    uint32_t offset = task->chunk_index * CHUNK_SIZE * INT_PER_LINE +
                      task->start_index * INT_PER_LINE;
    int32_t len;

    /* Stores data in the buffer in JSON format */
    uint32_t current_index = start_index;
    for (uint32_t i = 0; i < task->total_lines; i++) {
        len =
            snprintf(&buf_out[current_index], BUF_PER_LINE, format,
                     (task->chunk_index == 0 && current_index == 0) ? "" : ",",
                     &buf_in[pos[offset]], &buf_in[pos[offset + 1]],
                     &buf_in[pos[offset + 2]], &buf_in[pos[offset + 3]],
                     &buf_in[pos[offset + 4]], &buf_in[pos[offset + 5]],
                     &buf_in[pos[offset + 6]], &buf_in[pos[offset + 7]],
                     &buf_in[pos[offset + 8]], &buf_in[pos[offset + 9]],
                     &buf_in[pos[offset + 10]], &buf_in[pos[offset + 11]],
                     &buf_in[pos[offset + 12]], &buf_in[pos[offset + 13]],
                     &buf_in[pos[offset + 14]], &buf_in[pos[offset + 15]],
                     &buf_in[pos[offset + 16]], &buf_in[pos[offset + 17]],
                     &buf_in[pos[offset + 18]], &buf_in[pos[offset + 19]]);
        current_index += len;
        offset += INT_PER_LINE;
    }
    task->length = current_index - start_index;

    pthread_exit(NULL);
}
