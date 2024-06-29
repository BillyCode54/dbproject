#include <stdio.h>
#include <sqlite3.h>
 
#include <stdlib.h>
#include "LoadCsvFile.h"


#define MAX_NODES 2031337

const char *connectionStringSqlite = "test_database.db";
sqlite3 *db = NULL;


int openDatabase() {
    if (db) {
        fprintf(stderr, "Database connection already open.\n");
        return 1;
    }

    if (sqlite3_open(connectionStringSqlite, &db)) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    return 0;
}

void closeDatabase() {
    if (db) {
        sqlite3_close(db);
        db = NULL;
    }
}

int executeQuery(const char *query) {
    char *errMsg = 0;

    if (!db && openDatabase()) {
        return 1;
    }

    if (sqlite3_exec(db, query, NULL, 0, &errMsg)) {
        fprintf(stderr, "SQL error: %s\n", errMsg);
        sqlite3_free(errMsg);
        return 1;
    }

    return 0;
}

int findChildrenOfNode(char *nodeName) {
    sqlite3_stmt *stmt;
    int rc;


    char *query = "SELECT nn.nn_name FROM n_node nn JOIN n_relation nr ON nr.nn_children_id = nn.nn_id WHERE nr.nn_parent_id = (SELECT nn_id from n_node na where na.nn_name = 'nodeName' limit 1 );";
    query = replaceString(query, "nodeName", nodeName);
    // printf("dawd %s\n", query);
    
    // printf("%s\n", query);
    if (!db && openDatabase()) {
        return 1;
    }


    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Children of node '%s':\n", nodeName);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}
int countChildrenOfParent(char *nodeName) {
    sqlite3_stmt *stmt;
    int rc;


    char *query = "SELECT COUNT(*) FROM n_node nn JOIN n_relation nr ON nr.nn_children_id = nn.nn_id WHERE nr.nn_parent_id = (SELECT nn_id from n_node na where na.nn_name = 'nodeName' limit 1 );";
    query = replaceString(query, "nodeName", nodeName);


    if (!db && openDatabase()) {
        return 1;
    }

    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        int count = sqlite3_column_int(stmt, 0);
        printf("Number of children for parent node %s: %d\n", nodeName, count);
    }

    sqlite3_finalize(stmt);

    return 0;
}

int findGrandChildrenOfNode(char *nodeName) {
    sqlite3_stmt *stmt;
    int rc;


    char *query = "SELECT DISTINCT (nn_child.nn_name) from n_relation nr JOIN n_node nn_child ON (nn_child.nn_id = nr.nn_children_id) where nn_parent_id IN (SELECT nn_children_id from n_relation nr where nn_parent_id = (SELECT nn_id from n_node nn where nn_name = 'nodeName' limit 1));";
    query = replaceString(query, "nodeName", nodeName);

    if (!db && openDatabase()) {
        return 1;
    }
    
    

    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Grand children of node '%s':\n", nodeName);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}

int findParentsOfNode(char *nodeName) {
    sqlite3_stmt *stmt;
    int rc;


    char *query = "SELECT nn.nn_name FROM n_node nn JOIN n_relation nr ON nr.nn_parent_id = nn.nn_id WHERE nr.nn_children_id = (SELECT nn_id from n_node na where na.nn_name = 'nodeName' limit 1 );";
    query = replaceString(query, "nodeName", nodeName);

    if (!db && openDatabase()) {
        return 1;
    }
    
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Parents of node '%s':\n", nodeName);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}

int CountsParentsOfNode(char *nodeName) {
    sqlite3_stmt *stmt;
    int rc;


    char *query = "SELECT count(nn.nn_name) FROM n_node nn JOIN n_relation nr ON nr.nn_parent_id = nn.nn_id WHERE nr.nn_children_id = (SELECT nn_id from n_node na where na.nn_name = 'nodeName' limit 1 );";
    query = replaceString(query, "nodeName", nodeName);

    if (!db && openDatabase()) {
        return 1;
    }
    
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Counted parents of node '%s':\n", nodeName);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}

int findGrandParentsOfNode(char *nodeName) {
    sqlite3_stmt *stmt;
    int rc;


    char *query = "SELECT DISTINCT (nn_parent.nn_name) from n_relation nr JOIN n_node nn_parent ON (nn_parent.nn_id = nr.nn_parent_id) where nr.nn_children_id IN (SELECT nn_parent_id from n_relation nr where nn_children_id = (SELECT nn_id from n_node nn where nn_name = 'nodeName' limit 1));";
    query = replaceString(query, "nodeName", nodeName);

    if (!db && openDatabase()) {
        return 1;
    }
    
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Counted grand parents of node '%s':\n", nodeName);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}

int countUniquelyNamedNodes() {
    sqlite3_stmt *stmt;
    int rc;

    char *query = "SELECT COUNT(*) FROM n_node nn;";
  
    if (!db && openDatabase()) {
        return 1;
    }
    
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Counted of uniquely named nodes :\n");
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int nn_id = sqlite3_column_int(stmt, 0);
        // const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%d, ", nn_id);
    }
        printf("\n");


    return 0;
}

int findRootNodes() {
    sqlite3_stmt *stmt;
    int rc;

    char *query = "SELECT (nn.nn_name) FROM n_node nn WHERE nn.nn_id NOT IN (SELECT DISTINCT (nr.nn_children_id) FROM n_relation nr);";
  
    if (!db && openDatabase()) {
        return 1;
    }
    
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Counted grand parents of node :\n");
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}

int findMostChildrenNodes() {
    sqlite3_stmt *stmt;
    int rc;

    char *query = "SELECT nn.nn_name as children_count FROM n_node nn JOIN n_relation nr ON nn.nn_id = nr.nn_parent_id GROUP BY nn.nn_id, nn.nn_name ORDER BY COUNT(*) DESC LIMIT 1;";
  
    if (!db && openDatabase()) {
        return 1;
    }
    
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Nodes with most children:\n");
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}

int findLeastChildrenNodes() {
    sqlite3_stmt *stmt;
    int rc;

    char *query = "WITH ss AS(SELECT nr.nn_parent_id , COUNT(nr.nn_children_id) as cnt FROM n_relation nr GROUP BY nr.nn_parent_id) SELECT nn_name from ss s JOIN n_node nn on  (s.nn_parent_id = nn.nn_id) WHERE cnt = 1 ;";
  
    if (!db && openDatabase()) {
        return 1;
    }
    
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Nodes with least children:\n");
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}

int renameNodes(char *nodeName1, char *nodeName2) {
    sqlite3_stmt *stmt;
    int rc;

    char *query = "UPDATE n_node SET nn_name = 'nodeName2' WHERE nn_name = 'nodeName1';";
    query = replaceString(query, "nodeName1", nodeName1);
    query = replaceString(query, "nodeName2", nodeName2);

    if (!db && openDatabase()) {
        return 1;
    }
    
    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    printf("Node %s changed to %s \n", nodeName1, nodeName2);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        // int nn_id = sqlite3_column_int(stmt, 0);
        const char *nn_name = (const char *)sqlite3_column_text(stmt, 0);

        printf("%s, ", nn_name);
    }
        printf("\n");


    return 0;
}


#pragma region task12
void printPath(int path[], int pathIndex) {
    for (int i = 0; i <= pathIndex; i++) {
        printf("%d ", path[i]);
    }
    printf("\n");
}

int fetchNodeId(const char* nodeName) {

    char query[1000];
    snprintf(query, sizeof(query), "SELECT nn_id FROM n_node WHERE nn_name = '%s';", nodeName);

    sqlite3_stmt *stmt = NULL;

    if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL)!= SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return -1;
    }
    int result = sqlite3_step(stmt);
    if (result == SQLITE_ROW) {
        int nodeId = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
        return nodeId;
    } else {
        fprintf(stderr, "Failed to fetch node ID for node: %s\n", nodeName);
        sqlite3_finalize(stmt);
        return -1;
    }
}

int* getChildrenIds(int parentId) {
    sqlite3_stmt *stmt;
    int rc;
    char query[1024]; // Ensure buffer is large enough

    // Correctly format the query without modifying string literals
    sprintf(query, "SELECT nn_children_id FROM n_relation nr WHERE nr.nn_parent_id = %d;", parentId);

    if (!db && openDatabase()) {
        return NULL;
    }

    rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
    if (rc!= SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return NULL;
    }

    int numChildren = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        numChildren++;
    }

    // Allocate memory for children array
    int* children = malloc(numChildren * sizeof(int));
    if (!children) {
        fprintf(stderr, "Failed to allocate memory for children.\n");
        return NULL;
    }

    // Re-execute the query to fill the children array
    rc = sqlite3_reset(stmt); // Reset the statement
    if (rc!= SQLITE_OK) {
        fprintf(stderr, "Failed to reset statement: %s\n", sqlite3_errmsg(db));
        free(children); // Clean up allocated memory
        return NULL;
    }

    int i = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        children[i++] = sqlite3_column_int(stmt, 0);
    }

    return children;
}

void findAllPathsUtil(int start, int end, int path[], int pathIndex, int visited[]) {
    visited[start] = 1;
    path[pathIndex] = start;
    pathIndex++;

    if (start == end) {
        printPath(path, pathIndex - 1);
    } else {
        int* children = getChildrenIds(start);
        for (int i = 0; children[i] != -1; i++) {
            int child = children[i];
            if (!visited[child]) {
                findAllPathsUtil(child, end, path, pathIndex, visited);
            }
        }
        free(children);
    }

    pathIndex--;
    visited[start] = 0;
}

void findAllPaths(int start, int end) {
    int path[MAX_NODES];
    int visited[MAX_NODES] = {0};
    int pathIndex = 0;
    findAllPathsUtil(start, end, path, pathIndex, visited);
}


#pragma endregion

void FindAllPathsBetweenNodes(char *nodeName1, char *nodeName2) {
    int nodeStartId,nodeEndId;
    nodeStartId = fetchNodeId(nodeName1);
    nodeEndId = fetchNodeId(nodeName2);
    
    if (nodeStartId == -1 || nodeEndId == -1)
    {
        printf("Node not found\n");
        return;
    }
    findAllPaths(nodeStartId, nodeEndId);

}