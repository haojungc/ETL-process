#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>

#define BUF_SIZE (13 * 20)
#define INT_PER_LINE 20

static FILE *fp_in, *fp_out;
static uint32_t total_threads = 1;

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

    printf("Converting CSV to JSON with %u thread(s) ...\n", total_threads);

    pthread_t *t = malloc(total_threads * sizeof(pthread_t));

    /* Opens the input file */
    if (!(fp_in = fopen(input_filename, "r"))) {
        printf("Error: unable to open %s\n", input_filename);
        exit(EXIT_FAILURE);
    }

    /* Opens the output file */
    if (!(fp_out = fopen(output_filename, "w"))) {
        printf("Error: unable to open %s\n", input_filename);
        exit(EXIT_FAILURE);
    }
    fprintf(fp_out, "[");

    int32_t n[INT_PER_LINE];
    uint64_t total_lines = 0;
    /* TODO:
     * Analyze the efficiency for both fscanf and fgets */
    while (fscanf(fp_in,
                  "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d",
                  &n[0], &n[1], &n[2], &n[3], &n[4], &n[5], &n[6], &n[7], &n[8],
                  &n[9], &n[10], &n[11], &n[12], &n[13], &n[14], &n[15], &n[16],
                  &n[17], &n[18], &n[19]) == INT_PER_LINE) {
        total_lines++;

        fprintf(fp_out, "%s", total_lines == 1 ? "\n\t{\n" : ",\n\t{\n");

        for (uint8_t i = 1; i <= INT_PER_LINE; i++) {
            fprintf(fp_out, "\t\t\"col_%d\":%d%s", i, n[i - 1],
                    i == INT_PER_LINE ? "\n\t}" : ",\n");
        }
    }
    fprintf(fp_out, "\n]");

    free(t);
    fclose(fp_in);
    fclose(fp_out);

    end = clock();

    printf("Converted %lu lines of data.\n", total_lines);
    printf("Elapsed Time: %.2f secs\n", (double)(end - start) / CLOCKS_PER_SEC);

    return 0;
}