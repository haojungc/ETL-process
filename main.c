#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>

#define BUF_SIZE 200000000
#define INT_PER_LINE 20

static void convert_csv_to_json(const char *f_out, const char *f_in,
                                const uint32_t total_threads);

static FILE *fp_in, *fp_out;
int32_t n[BUF_SIZE];

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
    printf("Converting CSV to JSON with %u thread(s) ...\n", total_threads);
    fprintf(fp_out, "[");

    pthread_t *t = malloc(total_threads * sizeof(pthread_t));
    uint64_t total_lines = 0;
    uint64_t offset = 0;
    /* Reads from input file */
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
        offset += 20;
    }
    const char format[] = "%s\n"
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
    /* Writes to output file */
    offset = 0;
    for (uint64_t i = 0; i < total_lines; i++) {
        fprintf(fp_out, format, i == 0 ? "" : ",", n[offset], n[offset + 1],
                n[offset + 2], n[offset + 3], n[offset + 4], n[offset + 5],
                n[offset + 6], n[offset + 7], n[offset + 8], n[offset + 9],
                n[offset + 10], n[offset + 11], n[offset + 12], n[offset + 13],
                n[offset + 14], n[offset + 15], n[offset + 16], n[offset + 17],
                n[offset + 18], n[offset + 19]);
        offset += 20;
    }

    fprintf(fp_out, "\n]");

    free(t);
    fclose(fp_in);
    fclose(fp_out);
    printf("Converted %lu lines of data.\n", total_lines);
}
