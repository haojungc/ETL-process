CC := gcc
CFLAGS := -Wall -pthread -fopenmp -O3 -std=c99
EXE := convert

all:
	$(CC) main.c $(CFLAGS) -o $(EXE)
	$(CC) csv_generator.c $(CFLAGS) -o generator

.PHONY: clean

clean:
	rm -f $(EXE) generator
