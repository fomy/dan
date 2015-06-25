#include <stdio.h>
#include <string.h>

int main(){
    char str[] = "Min Fu is a PhD student.";
    char* tok = strtok(str, " ");
    while(tok != NULL){
        printf("%s\n", tok);
        tok = strtok(NULL, " ");
    }
        
    return 0;
}
