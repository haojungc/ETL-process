CC := gcc
CFLAGS := -Wall -O3
LDFLAGS := -lpthread

all:
	$(CC) $(CFLAGS) -o generator csv_generator.c $(LDFLAGS)

.PHONY: clean
clean:
	rm -f generator