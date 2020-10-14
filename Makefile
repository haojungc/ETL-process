CC := gcc
CFLAGS := -Wall -pthread -O3

all:
	$(CC) $(CFLAGS) -o generator csv_generator.c

.PHONY: clean
clean:
	rm -f generator
