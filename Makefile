CC := gcc
CFLAGS := -std=c99 -Wall -pthread -O0
EXE := convert

all:
	$(CC) main.c $(CFLAGS) -o $(EXE)
	$(CC) csv_generator.c $(CFLAGS) -o generate
	
debug:
	$(CC) main.c $(CFLAGS) -g -o $(EXE)

.PHONY: clean

clean:
	rm -f $(EXE) generate
