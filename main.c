#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define BUF_SIZE (13 * 20)
#define INT_PER_LINE 20

static FILE *fp_in, *fp_out;
static int32_t total_threads = 1;

int main(int argc, char *argv[]) {
    clock_t start, end;
    const char input_filename[] = "input.csv";
    const char output_filename[] = "output.json";

    start = clock();

    if (argc > 2) {
        puts("Error: too many arguments");
        printf("Usage: %s [number-of-threads]\n", argv[0]);
        exit(EXIT_FAILURE);
    } else if (argc == 2)
        total_threads = atoi(argv[1]);

    printf("Converting CSV to JSON with %d thread(s) ...\n", total_threads);

    pthread_t *t = malloc(total_threads * sizeof(pthread_t));

    /* Open input file */
    if (!(fp_in = fopen(input_filename, "r"))) {
        printf("Error: unable to open %s\n", input_filename);
        exit(EXIT_FAILURE);
    }

    /* Open output file */
    if (!(fp_out = fopen(output_filename, "w"))) {
        printf("Error: unable to open %s\n", input_filename);
        exit(EXIT_FAILURE);
    }
    fprintf(fp_out, "[\n");

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
        if (total_lines == 1)
            fprintf(fp_out, "\t{\n");
        else
            fprintf(fp_out, ",\n\t{\n");

        for (uint8_t i = 0; i < INT_PER_LINE; i++) {
            if (i == INT_PER_LINE - 1)
                fprintf(fp_out, "\t\t\"col_%d\":%d\n", i + 1, n[i]);
            else
                fprintf(fp_out, "\t\t\"col_%d\":%d,\n", i + 1, n[i]);
        }
        fprintf(fp_out, "\t}");
    }
    fprintf(fp_out, "\n]");

    free(t);
    fclose(fp_in);
    fclose(fp_out);

    end = clock();

    printf("Elapsed Time: %.2f secs\n", (double)(end - start) / CLOCKS_PER_SEC);

    return 0;
}