#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "LoadCsvFile.h"
#include "database.h"
#include <time.h>

int main(int argc, char *argv[]) {
    clock_t start, end;
    double cpu_time_used;

    start = clock();


    int arg1 = atoi(argv[1]);
    char *str1 = argv[2];
    char *str2 = argv[3];

    // printf("Integer: %d\n", arg1);
    // printf("String 1: %s\n", str1);
    // printf("String 2: %s\n", str2);
    
    //1. Finds all children of a given node
    if (argc == 3 && arg1 == 1) {
        findChildrenOfNode(str1);
    }
    //2. Counts all children of a given node
    if (argc == 3 && arg1 == 2) {
        countChildrenOfParent(str1);
    }
    //3. Finds all grand children of a given node
    if (argc == 3 && arg1 == 3) {
        findGrandChildrenOfNode(str1);
    }
    //4. Finds all parents of a given node
    if (argc == 3 && arg1 == 4) {
        findParentsOfNode(str1);
    }
    //5. Counts all parents of a given node
    if (argc == 3 && arg1 == 5) {
        CountsParentsOfNode(str1);
    }
    //6. Finds all grand parents of a given node
    if (argc == 3 && arg1 == 6) {
        findGrandParentsOfNode(str1);
    }
    //7. Counts how many uniquely named nodes there are
    if (argc == 2 && arg1 == 7) {
        countUniquelyNamedNodes(str1);
    }
    //8. Finds a root node, one which is not a subcategory of any other node
    if (argc == 2 && arg1 == 8) {
        findRootNodes(str1);
    }
    //9. Finds nodes with the most children, there could be more the one
    if (argc == 2 && arg1 == 9) {
        findMostChildrenNodes(str1);
    }
    //10. Finds nodes with the least children (number of children is greater than zero), there could be more the one
    if (argc == 2 && arg1 == 10) {
        findLeastChildrenNodes(str1);
    }
    //11. Renames a given node
    if (argc == 4 && arg1 == 11) {
        renameNodes(str1, str2);
    }
    //12. Finds all paths between two given nodes, with directed edges from the first to the second node
    if (argc == 4 && arg1 == 12) {
        FindAllPathsBetweenNodes(str1, str2);
    }
    //13. Loads csv file
    if (argc == 2 && arg1 == 13) {
        LoadCsvFile("taxonomy_iw.csv");
    }
    

    end = clock();

    cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;

    printf("Execution time: %f seconds\n", cpu_time_used);

    return 0;
}