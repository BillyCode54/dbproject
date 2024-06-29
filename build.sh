#!/bin/bash

gcc hashmap.c database.c LoadCsvFile.c main.c -o main -lsqlite3