// Copyright [2022] <Peter Bryant>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <strings.h>
#include <getopt.h>
#include <ctype.h>
#include <string.h>


int main(int argc, char* argv[]) {
  FILE *fstream;
  // ~~~0: Check for correct number of command line args ~~~
  if (argc != 3) {
    printf("wordle: invalid number of args\n");
    exit(1);
  }
  // ~~~1: Open file ~~~
  fstream = fopen(argv[1], "r");
  // 1.1 Check that openning file was successful
  if (fstream == NULL) {
    printf("wordle: cannot open file\n");
    exit(1);
  }
  // ~~~2: Read in data from file ~~~
  // 2.1 Declare Variables
  // 2.11 Contained in stack
  size_t len;  // The size in bytes of the block of MEM pointed to by line
  int index;  // Index of the null terminator for a given line
  char *nullTerminator = NULL;  // Pointer to '\0' in string pointed to by line
  char *contains = NULL;  //  Checks if string contains a char

  // 2.12 Contained in heap
  char *line = NULL;  // Pointer to the line read from getline()

  // 2.2 Read in lines from the file
  while ((len = getline(&line, &len, fstream)) != -1) {
    // 2.21 Trim end chars (ie. '\0' and '\n')
    nullTerminator = strchr(line, '\0');   // Get a pointer to '/0' in line
    index = (int)(nullTerminator - line);  // Compute the index of the '\0'

    // 2.22 Check that the current word is 5 chars
    if ((index-1) != 5) {
      continue;
    }
    // 2.23 Compare search term and the read line
    for (int i = 0 ; i < strlen(argv[2]); i++) {
      contains = strchr(line, *(argv[2]+i));
      if ((contains != NULL)) {
        break;
      }
    }
    // 2.24 If the word doesn't contain any letter from the search
    // word, print it
    if ((contains == NULL)) {
      printf("%s", line);
    }
  }
  // ~~~3: Close file and free memory ~~~
  free(line);
  fclose(fstream);
  return 0;
}


