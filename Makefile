CC := gcc
CFLAGS := -Wall -pthread -O3 -std=c99
EXE := convert

all:
	$(CC) $(CFLAGS) -o $(EXE) main.c
	$(CC) $(CFLAGS) -o generator csv_generator.c

.PHONY: clean

clean:
	rm -f $(EXE) generator
