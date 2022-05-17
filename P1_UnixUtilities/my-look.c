// Copyright [2022] <Peter Bryant>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <strings.h>
#include <getopt.h>
#include <ctype.h>
#include <string.h>


// ~~~ HELPER FUNCTION ~~~
// Function takes a char point to a string and
// removes all non-alphanumeric chars from it.
void removeNonAlphaNum(char *word) {
  int i, j;
  for (i = 0; word[i] != '\0'; ++i) {
    while (!((isalnum(word[i])) || (word[i] == '\0'))) {
      for (j = i; word[j] != '\0' ; ++j) {
        word[j] = word[j+1];
      }
        word[j]='\0';
    }
  }
}

int main(int argc, char* argv[]) {
  FILE *fstream;
  // ~~~0. Check for valid number of command line args~~~
  if ((argc < 2) || (6 < argc)) {
    printf("my-look: invalid command line\n");
    return(1);
    }
  // ~~~1 Optional Command Line Arguments~~~
  int opt;
  int f_flag = 0;
  while ((opt = getopt(argc, argv, "Vhf")) != -1) {
      switch (opt) {
        case 'V':
          printf("my-look from CS537 Spring 2022\n");
          return(0);
        case 'h':
          printf("HELP INFO: my-look shows all lines beginning"
          " with a given string.\nUse '-V' or '-f' followed by"
          "a file path to specify that you either need\n"
          "information about the utility or that you want"
          " to use a certain file to search,\n"
          "respectively. Use '-h' to repeat this dialogue.\n");
          return(0);
        case 'f':
          fstream = fopen(argv[2], "r");
          if (fstream == NULL) {  // If the file pointer is invalid
            printf("my-look: cannot open file\n");
            return(1);
          }
          f_flag = 1;  // Mark that the file was already openned
          break;
        default:  // If there was an option that wasn't 'V', 'h', or 'f'
          if ((opt != -1)) {
            printf("my-look: invalid command line\n");
            return(1);
          }
       }
  }
// ~~~2: If no file is specified, get words from STDIN~~~
  if (f_flag == 0) {
// 2.1 Declare Variables
    char* line = NULL;  // Pointer to MEM where a line will be read to
    char* newComp = NULL;  // Pointer to MEM, stores alphanumeric chars
    size_t nread;  // Size in bytes of the block of MEM *line points to

// 2.2 Print all strings that match the search term
    while ((nread = getline(&line, &nread, stdin)) != -1) {
      newComp = malloc((strlen(line)+1)*sizeof(char));

// 2.21 Copy the string pointed to by line to comp
      strcpy(newComp, line);

// 2.22 Call helper function to remove all non-alphanumeric
    // chars from string pointed to by comp
      removeNonAlphaNum(newComp);

// 2.23 Call lib function to compare the user provided search string
    // with all strings in the file. Note: Compares strings up to the length
    // of the user provided search term ( ie. strlen(argv[argc-1]) )
      int isEqual = strncasecmp(argv[argc-1], newComp, strlen(argv[argc-1]));

// 2.24 Print the string if it matches
      if (isEqual == 0) {
        printf("%s", line);
      }
      free(newComp);
    }
  }

  // ~~~3: Compare the strings in the file to the search string~~~
  if (f_flag == 1) {
// 3.1 Declare Variable
  size_t nread;  // Size in bytes of the block of MEM *line points to
  char* line = NULL;  // Pointer to MEM where a line will be read to
  char* comp = NULL;  // Pointer to MEM, stores alphanumeric chars

// 3.2 Print strings that match the search term
  while ((nread = getline(&line, &nread, fstream))!= -1) {
    // 3.21 Allocated memory for comp that is one larger than line
    comp = malloc((strlen(line)+1)*sizeof(char));

    // 3.22 Copy the string pointed to by line to comp
    strcpy(comp, line);

    // 3.23 Call helper function to remove all non-alphanumeric
    // chars from string pointed to by comp
    removeNonAlphaNum(comp);

    // 3.24 Call lib function to compare the user provided search string
    // with all strings in the file. Note: Compares strings up to the length
    // of the user provided search term ( ie. strlen(argv[argc-1]) )
    int isEqual = strncasecmp(argv[argc-1], comp, strlen(argv[argc-1]));

    // 3.25 Print the string if it matches
    if (isEqual == 0) {
      printf("%s", line);
    }
    free(comp);
  }

  // 3.3 Close file and free memory
  free(line);
  fclose(fstream);
  }
  return 0;
}

