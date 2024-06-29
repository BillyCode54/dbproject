#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sqlite3.h>
#include "database.h"
#include "hashmap.h"

#define NODES_NUM 2031337
#define MAX_SIZE 2500000

sqlite3 *db;

struct StringPair {
    char* first;
    char* second;
};

int openDatabase();
void closeDatabase();
int executeQuery(const char* query);
int fetchNodeId(const char* nodeName, int* nodeId);

Hashmap* map; // Declare a global hashmap
int indexNode; // Global index tracker

void InitHashMap() {
    map = hashmap_create(MAX_SIZE); // Initialize hashmap with maximum size
    if (map == NULL) {
        fprintf(stderr, "Failed to initialize hashmap\n");
        exit(EXIT_FAILURE);
    }
    printf("Initialize hashmap\n");
}

int getIndex(char key[]) {
    return hashmap_get(map, key); // Use hashmap get function
}

void insert(char key[], int value) {
    hashmap_put(map, key, value); // Use hashmap put function
}

int get(char key[]) {
    return hashmap_get(map, key); // Use hashmap get function
}

char* replaceString(const char* s, const char* oldW, const char* newW) {
    char* result; 
    int i, cnt = 0; 
    int newWlen = strlen(newW); 
    int oldWlen = strlen(oldW); 

    for (i = 0; s[i] != '\0'; i++) { 
        if (strstr(&s[i], oldW) == &s[i]) { 
            cnt++; 
            i += oldWlen - 1; 
        } 
    } 

    result = (char*)malloc(i + cnt * (newWlen - oldWlen) + 1); 
    if (result == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        exit(EXIT_FAILURE);
    }

    i = 0; 
    while (*s) { 
        if (strstr(s, oldW) == s) { 
            strcpy(&result[i], newW); 
            i += newWlen; 
            s += oldWlen; 
        } else {
            result[i++] = *s++; 
        }
    } 

    result[i] = '\0'; 
    return result; 
}

char** ParseCsvLine(const char* csv) {
    char* pair[2];

    char str[250];
    char *part1, *part2;

    strncpy(str, csv, sizeof(str));
    str[sizeof(str) - 1] = '\0';

    char *delimiter = strstr(str, "\",\"");

    if (delimiter != NULL) {
        *delimiter = '\0';
        part1 = str + 1;
        part2 = delimiter + 3;

        part2[strlen(part2) - 2] = '\0';

        pair[0] = part1;
        pair[1] = part2;
    } else {
        printf("Delimiter not found in the string.\n");
    }

    pair[0] = replaceString(pair[0], "\\\"", "\"");
    pair[0] = replaceString(pair[0], "\'", "\'\'");
    pair[1] = replaceString(pair[1], "\\\"", "\"");
    pair[1] = replaceString(pair[1], "\'", "\'\'");

    char** result = malloc(2 * sizeof(char*));
    if (result != NULL) {
        result[0] = pair[0];
        result[1] = pair[1];
    }
    return result;
}

void InitDBQueries() {
    printf("Initializing db\n");

    const char* query = 
    "BEGIN;\n"
    "DROP TABLE IF EXISTS n_relation;\n"
    "DROP TABLE IF EXISTS n_node;\n"
    "COMMIT;\n";
    executeQuery(query);

    query = 
        "CREATE TABLE IF NOT EXISTS n_node (\n"
        "    nn_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
        "    nn_name TEXT \n"
        ");\n"
        "CREATE TABLE IF NOT EXISTS n_relation (\n"
        "    nn_parent_id INTEGER,\n"
        "    nn_children_id INTEGER,\n"
        "    PRIMARY KEY(nn_parent_id, nn_children_id),\n"
        "    FOREIGN KEY(nn_parent_id) REFERENCES n_node(nn_id),\n"
        "    FOREIGN KEY(nn_children_id) REFERENCES n_node(nn_id)\n"
        ");\n";
    executeQuery(query);
}



void InsertRelationsToDatabase(char *relations[][2], int numPairs) {
    char insertNodeQuery[1000];
    char insertRelationQuery[1000];
    int res, parentID, childID;

    res = executeQuery("BEGIN;");
    if (res != 0) {
        fprintf(stderr, "Failed to begin transaction\n");
        return;
    }

    for (int i = 0; i < numPairs; i++) {
        parentID = get(relations[i][0]);
        if (parentID == -1) {
            indexNode ++;
            insert(relations[i][0],indexNode);
            snprintf(insertNodeQuery, sizeof(insertNodeQuery), "INSERT INTO n_node(nn_id,nn_name) VALUES (%d,'%s');", indexNode,relations[i][0]);
            res = executeQuery(insertNodeQuery);
            if (res != 0) {
                fprintf(stderr, "Failed to insert node: %s\n", relations[i][0]);
                continue;
            }
            parentID = indexNode;
        }

        childID = get(relations[i][1]);
        if (childID == -1) {
            indexNode ++;
            insert(relations[i][1],indexNode);
            snprintf(insertNodeQuery, sizeof(insertNodeQuery), "INSERT INTO n_node(nn_id,nn_name) VALUES (%d,'%s');", indexNode,relations[i][1]);
            res = executeQuery(insertNodeQuery);
            if (res != 0) {
                fprintf(stderr, "Failed to insert node: %s\n", relations[i][1]);
                continue;
            }
            childID = indexNode;
        }

        snprintf(insertRelationQuery, sizeof(insertRelationQuery), "INSERT INTO n_relation(nn_parent_id, nn_children_id) VALUES (%d, %d);", parentID, childID);
        res = executeQuery(insertRelationQuery);
        if (res != SQLITE_OK && res != SQLITE_DONE) {
            fprintf(stderr, "Failed to insert relation: %s %d -> %s %d\n", relations[i][0],parentID, relations[i][1], childID);
        }
    }

    res = executeQuery("COMMIT;");
    if (res != 0) {
        printf("Failed to commit transaction\n");
    }
}


void InsertInBatches(FILE *file, int num_pairs, int num_pairs_max, int i) {
    char* relations[num_pairs][2];
    char line[1000];
    int linecount = 0;
    static int totalLinesParsed = 0;

    while (fgets(line, sizeof(line), file) != NULL) {
        char** pair = ParseCsvLine(line);
        if (pair == NULL) {
            continue;
        }
        relations[linecount][0] = pair[0];
        relations[linecount][1] = pair[1];
        linecount++;
        totalLinesParsed++;

        if (linecount >= num_pairs) {
            break;
        }
    }
    // printf("Number of lines parsed: %d\n", totalLinesParsed);
    InsertRelationsToDatabase(relations, linecount);
}

void InsertIndexesIntoDB(){
    char *query = 
        "CREATE INDEX IF NOT EXISTS idx_n_node_nn_name ON n_node(nn_name);\n"
        "CREATE INDEX IF NOT EXISTS idx_child_id ON n_relation(nn_children_id);\n"
        "CREATE INDEX IF NOT EXISTS idx_parent_id ON n_relation(nn_parent_id);";
        executeQuery(query);
}

void LoadCsvFile(char *fileName) {
    printf("Loading csv file\n");
    if (openDatabase()) {
        fprintf(stderr, "Failed to open database\n");
        return;
    }

    InitHashMap();

    InitDBQueries();
    FILE *file = fopen(fileName, "r");
    if (file == NULL) {
        printf("Failed to open file: %s\n", strerror(errno));
        return;
    }

    int num_pairs_max = 5771611;
    int num_pairs = 500000;

    for (int i = 0; i < num_pairs_max; i += num_pairs) {
        InsertInBatches(file, num_pairs, num_pairs_max, i);
        double percentage = ((double)i / num_pairs_max) * 100;
         printf("Progress: %.2f%%\n", percentage);
    }

    fclose(file);

    InsertIndexesIntoDB();

    closeDatabase();

    hashmap_destroy(map);
}

