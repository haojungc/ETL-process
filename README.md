---
tags: operating systems
---
# 2020_OS_Fall_HW2: ETL process

[![hackmd-github-sync-badge](https://hackmd.io/QGd_yHkBRXqHoNb7hBVycg/badge)](https://hackmd.io/QGd_yHkBRXqHoNb7hBVycg)

contributed by < `nickchenchj` >
> related links: 
> * [2020_OS_Fall_HW2: ETL process (English)](https://hackmd.io/@dsclab/B1cxhvp8w)
> * [2020_OS_Fall_HW2: ETL process (中文)](https://hackmd.io/@dsclab/rktgrMOUD)

## Goals
* Write a Multi-Thread program that can convert a CSV file to a JSON file.
* Observe and analyze the usage of system resources (such as CPU, Memory, Disk I/O, etc.) to find out how the operating system works for our programs.

### Test Data
* Please generate a **single file** that contains `N` lines of data, each data contains `20` random numbers.
* Random value range: `-2147483648` ~ `2147483647`, i.e. -2^31^ ~ 2^31^-1 (4-byte signed integer)
* Use `|` to split random numbers; use newline character ([LF](https://en.wikipedia.org/wiki/Newline)) to split data.
* Your program should be able to handle data that is at least 1 GB in size.
* Use `UTF-8` encoding , and name the input data file as `input.csv`.

The test data you generate (`input.csv`) should be like:
```csv 
1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20
2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21
3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22
4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23
...
```

### CSV to JSON Converter
1. The converter must be able to correctly convert data in a CSV file to JSON format.
    * The input file should be named `input.csv`.
    * The output file should be named `output.json`.
2. The converter must be developed with **Multi-Thread** and include a `threads` argument in the execution command to dynamically adjust the number of threads used by the program.
3. The converter must be able to process input data that meets the test data criteria as defined in the [previous paragraph](#Test-Data).
    * The input file contains `N` lines of data.
    * Each data contains `20` random numbers.
    * Random numbers are seperated by `|`.
4. Your program should be able to handle at least 1 GB of data.
5. Use `col_{INDEX}` as the `KeyName` when converting data to JSON format.
6. The data converted to JSON format must be in the same order as the input CSV file, meaning that the first line of CSV is the first object of JSON.
7. Do not include the procedure for generating test data in the conversion program. Because TAs will use their own input data (`input.csv`).
8. **(Optional)** You can also use GPU to develop CUDA programs for this homework.

#### Input Example (`input.csv`):
```csv=
1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20
2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21
3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22
4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23
```

#### Output Example (`output.json`):
:::spoiler
```json=
[
   {
      "col_1":1,
      "col_2":2,
      "col_3":3,
      "col_4":4,
      "col_5":5,
      "col_6":6,
      "col_7":7,
      "col_8":8,
      "col_9":9,
      "col_10":10,
      "col_11":11,
      "col_12":12,
      "col_13":13,
      "col_14":14,
      "col_15":15,
      "col_16":16,
      "col_17":17,
      "col_18":18,
      "col_19":19,
      "col_20":20
   },
   {
      "col_1":2,
      "col_2":3,
      "col_3":4,
      "col_4":5,
      "col_5":6,
      "col_6":7,
      "col_7":8,
      "col_8":9,
      "col_9":10,
      "col_10":11,
      "col_11":12,
      "col_12":13,
      "col_13":14,
      "col_14":15,
      "col_15":16,
      "col_16":17,
      "col_17":18,
      "col_18":19,
      "col_19":20,
      "col_20":21
   },
   {
      "col_1":3,
      "col_2":4,
      "col_3":5,
      "col_4":6,
      "col_5":7,
      "col_6":8,
      "col_7":9,
      "col_8":10,
      "col_9":11,
      "col_10":12,
      "col_11":13,
      "col_12":14,
      "col_13":15,
      "col_14":16,
      "col_15":17,
      "col_16":18,
      "col_17":19,
      "col_18":20,
      "col_19":21,
      "col_20":22
   },
   {
      "col_1":4,
      "col_2":5,
      "col_3":6,
      "col_4":7,
      "col_5":8,
      "col_6":9,
      "col_7":10,
      "col_8":11,
      "col_9":12,
      "col_10":13,
      "col_11":14,
      "col_12":15,
      "col_13":16,
      "col_14":17,
      "col_15":18,
      "col_16":19,
      "col_17":20,
      "col_18":21,
      "col_19":22,
      "col_20":23
   }
]
```
:::

## Environment
* OS: Ubuntu 20.04.1 LTS
* Arch: X86-64
* CPU: Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz
* Ｍemory: 8 GB
* Programming Language: C

## Usage
```bash 
# Compile
# note: all codes are compiled with -O0
make

# Generate random integers and store them in a CSV file
# e.g. ./generate --lines=1000000 --thread=10
# default number of lines: 5,000,000
# default number of threads: 1
./generate [--lines=<number-of-lines>] [--thread=<number-of-threads>]

# Convert data in input.csv from CSV to JSON format
# and store the converted data in output.json
# e.g. ./convert --thread=2
# default number of threads: 1
./convert [--thread=<number-of-threads>]

# Remove executable files
make clean
```

## Development Process
* [GitHub Repo](https://github.com/nickchenchj/ETL-process)

### Idea
1. Implement single-threaded converter and optimize it.
2. Implement multi-threaded converter.
3. Reduce execution time and memory usage.

### Single-threaded Implementation and Optimization
* use `perf` to analyze the converter:
```bash 
# Record
perf record -F 99 -g ./convert --thread=1
# Show the performance result
perf report --no-children
```

#### Input Data
* Total Lines: `5,000,000`

#### Optimization
**Test 1**
* code snippet (original):
```c=
...
fprintf(fp_out, "[");
int32_t n[INT_PER_LINE];
uint64_t total_lines = 0;

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
...
```

* Test 1.1:

![](https://i.imgur.com/WnWyq5Q.png)

![](https://i.imgur.com/4CM4ixe.png)


* Test 1.2:

![](https://i.imgur.com/x4orHpO.png)

![](https://i.imgur.com/GXfsOms.png)


* Test 1.3:

![](https://i.imgur.com/XJB9EeA.png)

![](https://i.imgur.com/UyfSEoF.png)


|    Test     | CPU Time Used (s) |
|:-----------:|:-----------------:|
|     1.1     |       34.37       |
|     1.2     |       34.48       |
|     1.3     |       35.06       |
| **Average** |     **34.64**     |

* It is clear that the program spent most time on `fprintf`, so we replace some `fprintf` with `snprintf` and `fprintf`.

**Test 2**
* code snippet:
```c=
...
fprintf(fp_out, "[");
int32_t n[INT_PER_LINE];
uint64_t total_lines = 0;
char buf[BUF_SIZE];

while (fscanf(fp_in,
              "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d",
              &n[0], &n[1], &n[2], &n[3], &n[4], &n[5], &n[6], &n[7], &n[8],
              &n[9], &n[10], &n[11], &n[12], &n[13], &n[14], &n[15], &n[16],
              &n[17], &n[18], &n[19]) == INT_PER_LINE) {
    total_lines++;

    snprintf(buf, BUF_SIZE,
             "%s\n"
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
             "\t}",
             total_lines == 1 ? "" : ",", n[0], n[1], n[2], n[3], n[4],
             n[5], n[6], n[7], n[8], n[9], n[10], n[11], n[12], n[13],
             n[14], n[15], n[16], n[17], n[18], n[19]);
    fprintf(fp_out, "%s", buf);
}
fprintf(fp_out, "\n]");
...
```
* Test 2.1:

![](https://i.imgur.com/du2BQnk.png)

![](https://i.imgur.com/zYaNn9v.png)


* Test 2.2:

![](https://i.imgur.com/4eodTOg.png)

![](https://i.imgur.com/0xCxiZg.png)


* Test 2.3:

![](https://i.imgur.com/cxECxg1.png)

![](https://i.imgur.com/5Wb03Ri.png)

|       Test        | CPU Time Used (s) |
|:-----------------:|:-----------------:|
|        2.1        |       28.32       |
|        2.2        |       28.06       |
|        2.3        |       27.82       |
|    **Average**    |     **28.07**     |
| **Average (old)** |     **34.64**     |
|    **Speedup**    |    **+23.4 %**    |

###### Note: $Speedup = (\frac{Average_{old}}{Average_{new}} - 1) \times 100\%$, $Speedup > 1$: faster, $Speedup < 1$: slower

* From the result above, we can see that `fscanf` is the most time-consuming function, so we replace `fprintf` with `fputs`.

**Test 3**
* code snippet:
```c=
...
fprintf(fp_out, "[");
int32_t n[INT_PER_LINE];
uint64_t total_lines = 0;
char buf[BUF_SIZE];

while (fscanf(fp_in,
              "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d",
              &n[0], &n[1], &n[2], &n[3], &n[4], &n[5], &n[6], &n[7], &n[8],
              &n[9], &n[10], &n[11], &n[12], &n[13], &n[14], &n[15], &n[16],
              &n[17], &n[18], &n[19]) == INT_PER_LINE) {
    total_lines++;

    snprintf(buf, BUF_SIZE,
             "%s\n"
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
             "\t}",
             total_lines == 1 ? "" : ",", n[0], n[1], n[2], n[3], n[4],
             n[5], n[6], n[7], n[8], n[9], n[10], n[11], n[12], n[13],
             n[14], n[15], n[16], n[17], n[18], n[19]);
    fputs(buf, fp_out);
}
fprintf(fp_out, "\n]");
...
```
* Test 3.1:

![](https://i.imgur.com/FjMYgw4.png)

![](https://i.imgur.com/ZHsXdPE.png)

* Test 3.2:

![](https://i.imgur.com/c4cico0.png)

![](https://i.imgur.com/9QDokb6.png)

* Test 3.3:

![](https://i.imgur.com/NbRV50r.png)

![](https://i.imgur.com/OsjXSQg.png)

|       Test        | CPU Time Used (s) |
|:-----------------:|:-----------------:|
|        3.1        |       28.57       |
|        3.2        |       28.91       |
|        3.3        |       28.20       |
|    **Average**    |     **28.56**     |
| **Average (old)** |     **28.07**     |
|    **Speedup**    |    **-1.7 %**     |

* `buf` seems a little bit redundant, so we replace `snprintf` with `fprintf` and declare a format string.

**Test 4**
* code snippet:
```c=
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
...
fprintf(fp_out, format, total_lines == 1 ? "" : ",", n[0], n[1], n[2],
        n[3], n[4], n[5], n[6], n[7], n[8], n[9], n[10], n[11], n[12],
        n[13], n[14], n[15], n[16], n[17], n[18], n[19]);
...
```
* Test 4.1:

![](https://i.imgur.com/vVpdtZq.png)

![](https://i.imgur.com/UYnabAH.png)

* Test 4.2:

![](https://i.imgur.com/wHC42pd.png)

![](https://i.imgur.com/elojBqu.png)

* Test 4.3:

![](https://i.imgur.com/uI1XFtW.png)

![](https://i.imgur.com/YxjcbGk.png)

|       Test        | CPU Time Used (s) |
|:-----------------:|:-----------------:|
|        4.1        |       27.54       |
|        4.2        |       27.75       |
|        4.3        |       27.09       |
|    **Average**    |     **27.46**     |
| **Average (old)** |     **28.56**     |
|    **Speedup**    |    **+4.0 %**     |

* We wonder whether switching between `fscanf` and `fprintf` has any effect on the performance, so we use a large integer array to store `input.csv` instead of processing 20 integers in each iteration.

**Test 5**
* code snippet:
```c=
#define BUF_SIZE 200000000
int32_t n[BUF_SIZE];

static void convert_csv_to_json(const char *f_out, const char *f_in,
                                const uint32_t total_threads)
{
...
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
    ...
}
```

* Test 5.1:

![](https://i.imgur.com/hW6HY9D.png)

![](https://i.imgur.com/N2wfFZv.png)


* Test 5.2:

![](https://i.imgur.com/l33HIfF.png)

![](https://i.imgur.com/NcHuotD.png)

* Test 5.3:

![](https://i.imgur.com/PsZEkHm.png)

![](https://i.imgur.com/advJWFD.png)

|       Test        | CPU Time Used (s) |
|:-----------------:|:-----------------:|
|        5.1        |       27.96       |
|        5.2        |       28.45       |
|        5.3        |       28.90       |
|    **Average**    |     **28.44**     |
| **Average (old)** |     **27.46**     |
|    **Speedup**    |    **-3.4 %**     |

* We can see from the result that switching between `fscanf` and `fprintf` doesn't really affect the performance.
* However, `fscanf` is still the most time-consuming function in the program, so we replace `fscanf` with `fread` when reading `input.csv`.

**Test 6**
* Use `fread` to store `input.csv` in a char array and convert the char array to an integer array one by one.
* code snippet of `read_data`:
```c=
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

    size_t result = fread(buf_in, sizeof(char), filesize, fp_in);
    if (result != filesize) {
        printf("error: fread failed\n");
        exit(EXIT_FAILURE);
    }
    fclose(fp_in);
    /* Assures buf_in ends with a newline character */
    buf_in[filesize] = '\n';
    size_t addend = (buf_in[filesize - 1] == '\n') ? 0 : 1;
    uint64_t total_lines =
        convert_binary_to_int32(filesize + addend, total_threads);
    free(buf_in);

    return total_lines;
}
```
* code snippet of `convert_binary_to_int32`:
```c= 
static uint64_t convert_binary_to_int32(const size_t size,
                                        const uint32_t total_threads) {
    uint64_t count = 0;
    uint64_t line_count = 0;
    uint64_t start = 0;
    int64_t num;
    bool is_negative;
    for (uint64_t i = 0; i < size; i++) {
        switch (buf_in[i]) {
        case '\n':
            line_count++;
        case '|':
            num = 0;
            is_negative = (buf_in[start] == '-') ? true : false;
            for (uint64_t j = start + (is_negative ? 1 : 0); j < i; j++) {
                num = (num << 3) + (num << 1) + (buf_in[j] - '0');
            }
            n[count++] = is_negative ? ~num + 1 : num;
            start = i + 1;
        }
    }
    printf("  Total Lines: %lu\n"
           "  Total Integers: %lu\n",
           line_count, count);

    return line_count;
}
```
* Test 6.1:

![](https://i.imgur.com/VgrCVEH.png)

![](https://i.imgur.com/cgEBRmh.png)

* Test 6.2:

![](https://i.imgur.com/eSKryBS.png)

![](https://i.imgur.com/mQriSOQ.png)


* Test 6.3:

![](https://i.imgur.com/6NBjUEJ.png)

![](https://i.imgur.com/eVX2Jfk.png)

|       Test        | CPU Time Used (s) |
|:-----------------:|:-----------------:|
|        6.1        |       25.12       |
|        6.2        |       26.19       |
|        6.3        |       25.12       |
|    **Average**    |     **25.48**     |
| **Average (old)** |     **28.44**     |
|    **Speedup**    |    **+11.6 %**    |

* It significantly speeded up! Let's replace `fprintf` with `fwrite` as well.

**Test 7**
* Original:
    1. stores all integers in an integer array
    2. writes all integers to output file in JSON format
* New:
    1. stores all starting positions of each integers to an integer array
    2. formats integers with `sprintf` (JSON)
    3. writes the formatted string to output file with `fwrite`
* code snippet of `read_data`:
```c= 
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
```
* code snippet of `set_pos`:
```c=
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
```
```c=
static void write_data(const uint64_t total_lines) {
    static const char format[] = JSON_FORMAT;
    char str[BUF_PER_LINE];
    uint32_t offset = 0;

    /* Writes data to output file in JSON format */
    for (uint32_t i = 0; i < total_lines; i++) {
        snprintf(str, BUF_PER_LINE, format, i == 0 ? "" : ",",
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
        fwrite(str, sizeof(char), strlen(str), fp_out);
        offset += INT_PER_LINE;
    }
}
```
* Test 7.1:

![](https://i.imgur.com/f2DNrzD.png)

![](https://i.imgur.com/yX2fJJ3.png)


* Test 7.2:

![](https://i.imgur.com/ZyMIUdX.png)

![](https://i.imgur.com/hVg8i3l.png)


* Test 7.3:

![](https://i.imgur.com/RjxQiJq.png)

![](https://i.imgur.com/QjfNaCl.png)


|       Test        | CPU Time Used (s) |
|:-----------------:|:-----------------:|
|        7.1        |       24.35       |
|        7.2        |       24.31       |
|        7.3        |       23.76       |
|    **Average**    |     **24.14**     |
| **Average (old)** |     **25.48**     |
|    **Speedup**    |    **+5.6 %**     |

* The single-threaded converter has speeded up from $34.64 s$ to $24.14 s$, $Speedup = +43.5 \%$. That's quite a lot, so let's move on to implementing and optimizing multi-threaded converter.

### Multi-threaded Implementation and Optimization
#### Idea
* Since reading from `input.csv` is fast, so we parallelize file writing first.
* We declare a large `char` array called `buf_out`.
* Assign `CHUNK_SIZE / total_threads` lines of data to each thread, then each thread will convert their lines to JSON format and store them in `buf_out`.
* After all threads finish their job, write `buf_out` to `output.json` and repeat the previous step until all lines are converted.

#### Implementation
* `task_t`:
```c=
typedef struct {
    uint32_t chunk_index;
    uint32_t start_index;
    uint32_t total_lines;
} task_t;
```

* code snippet:
```c=
#include <pthread.h>

#define MIN(x, y) ((x) < (y) ? (x) : (y))

#define CHUNK_SIZE 2000000
#define INT_PER_LINE 20
#define BUF_PER_INT 25

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
            fwrite(&buf_out[start_index], sizeof(char),
                   strlen(&buf_out[start_index]), fp_out);
        }
        total_lines -= current_chunk_size;
        chunk_index++;
    }
    free(t);
    free(task);
}
```
* code snippet of `convert_line`:
```c=
static void *convert_line(void *_task) {
    static const char format[] = JSON_FORMAT;
    task_t *task = (task_t *)_task;
    uint32_t buf_index = task->start_index * INT_PER_LINE * BUF_PER_INT;
    uint32_t offset = task->chunk_index * CHUNK_SIZE * INT_PER_LINE +
                      task->start_index * INT_PER_LINE;
    int32_t len;

    /* Stores data in the buffer in JSON format */
    for (uint32_t i = 0; i < task->total_lines; i++) {
        len = snprintf(&buf_out[buf_index],
                       INT_PER_LINE * BUF_PER_INT * sizeof(char), format,
                       (task->chunk_index == 0 && buf_index == 0) ? "" : ",",
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
        buf_index += len;
        offset += INT_PER_LINE;
    }

    pthread_exit(NULL);
}
```

#### Analysis
**Thread = 1**
![](https://i.imgur.com/oaYlbEc.png)

![](https://i.imgur.com/suAeS39.png)


|      Event       |     Result      |
|:----------------:|:---------------:|
|   cache-misses   |   2,1006,3356   |
| cache-references |   6,3547,9711   |
|   instructions   |  727,2005,4586  |
|      cycles      |  412,4888,4418  |
|   page-faults    |     36,5854     |
|   elapsed time   | 76.18 +- 4.46 s |


**Thread = 2**
![](https://i.imgur.com/NY0OxFo.png)

![](https://i.imgur.com/vHbW8Dk.png)

|      Event       |      Result       |
|:----------------:|:-----------------:|
|   cache-misses   |    3,4658,1638    |
| cache-references |    4,8700,6812    |
|   instructions   |   691,3230,7026   |
|      cycles      |   349,6712,5418   |
|   page-faults    |      59,4141      |
|   elapsed time   | 72.354 +- 0.695 s |

**Thread = 4**
![](https://i.imgur.com/wyGgIbl.png)

![](https://i.imgur.com/owtbNiC.png)


|      Event       |     Result      |
|:----------------:|:---------------:|
|   cache-misses   |   3,4176,2815   |
| cache-references |   4,8678,0945   |
|   instructions   |  688,7844,8276  |
|      cycles      |  355,1535,9411  |
|   page-faults    |     59,4149     |
|   elapsed time   | 70.81 +- 3.46 s |

**Thread = 8**
![](https://i.imgur.com/7b6cGxO.png)

![](https://i.imgur.com/Bss3BIa.png)


|      Event       |      Result       |
|:----------------:|:-----------------:|
|   cache-misses   |    3,4663,7147    |
| cache-references |    4,9791,3378    |
|   instructions   |   689,5626,9866   |
|      cycles      |   486,2594,4141   |
|   page-faults    |      59,4176      |
|   elapsed time   | 70.706 +- 0.755 s |

**Thread = 16**
![](https://i.imgur.com/VhSZpYI.png)

![](https://i.imgur.com/etvmfTM.png)


|      Event       |     Result      |
|:----------------:|:---------------:|
|   cache-misses   |   3,4909,5363   |
| cache-references |   5,0259,3780   |
|   instructions   |  689,3921,0800  |
|      cycles      |  483,7424,7118  |
|   page-faults    |     59,4236     |
|   elapsed time   | 70.81 +- 1.05 s |

**Thread = 32**
![](https://i.imgur.com/7gbitza.png)

![](https://i.imgur.com/wilqvZR.png)

|      Event       |     Result      |
|:----------------:|:---------------:|
|   cache-misses   |   3,4858,7516   |
| cache-references |   5,0563,0357   |
|   instructions   |  689,5591,9580  |
|      cycles      |  486,4064,3319  |
|   page-faults    |     59,4346     |
|   elapsed time   | 76.03 +- 3.01 s |

* From the results above, we noticed that all elapsed time are similar even when the program used more threads.
* We speculate that it might be related to the virtual memory, we can see that the page fault is `36,5854` with single thread while it is about `59,4100` with multiple threads. That is, when the program runs parallel, more data is stored in the disk rather than the RAM.
* Moreover, only `convert_line` is multi-threaded, but writing to output file isn't. If writing data is more time-consuming than `convert_line`, then parallelize `convert_line` won't be enough to enhance the performance.

#### Optimization
* use `perf record` to find time-consuming functions in the program:
```bash 
perf record -F 99 -g ./convert --thread=2
```
![](https://i.imgur.com/osoIpd2.png)

* add `length` to `task_t` to replace redundant `strlen` when writing data to output file:
* `task_t` (new):
```c=
typedef struct {
    uint32_t chunk_index;
    uint32_t start_index;
    uint32_t total_lines;
    size_t length;
} task_t;

/* Original */
fwrite(&buf_out[start_index], sizeof(char), strlen(&buf_out[start_index]), fp_out);

/* New */
fwrite(&buf_out[start_index], sizeof(char), task[i].length, fp_out);
```
* result:

![](https://i.imgur.com/xspbyOs.png)

* The cycles has reduced from `9.52%` to `6.79%`.

## Reference
* [Introduction to Parallel Computing](https://computing.llnl.gov/tutorials/parallel_comp/)
* [POSIX Threads Programming](https://computing.llnl.gov/tutorials/pthreads/)
* [Massif: a heap profiler](https://valgrind.org/docs/manual/ms-manual.html)
