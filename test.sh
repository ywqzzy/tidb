#!/bin/bash

# 数据库连接信息
DB_HOST="127.0.0.1"
DB_PORT="4000"
DB_USER="root"
DB_NAME="test"  # 替换为您的数据库名称

# 并行执行任务的函数
execute_task() {
    local db="$1"
    local query="$2"
    mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -D "$db" -e "$query"
}

# 生成创建表的 SQL 语句
generate_create_query() {
    local table_name="$1"
    local create_query="CREATE TABLE IF NOT EXISTS $table_name ("

    for ((i=1; i<=1000; i++)); do
        if [[ $i -eq 1000 ]]; then
            create_query+="col$i INT)"
        else
            create_query+="col$i INT,"
        fi
    done

    echo "$create_query"
}

# 删除库
drop_database() {
    local db="$1"
    mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -e "drop database $db"
    echo "库 $db 删除完成"
}

# 创建库
create_database() {
    local db="$1"
    mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -e "create database $db"
    echo "库 $db 创建完成"
}

create_table() {
    execute_task "$1" "$2"
    echo "表创建完成"
}

# 删除表
drop_table() {
    local table_name="$1"
    local drop_query="DROP TABLE IF EXISTS $table_name"

    execute_task "$drop_query"

    echo "表 $table_name 删除完成"
}

# 并行执行创建表和删除表的任务
main() {
    batch_size=10

    declare -a databases=()
    for ((i=1; i<=50;i++)); do
        local db_name="db$i"
        databases+=("$db_name")
        drop_database $db_name
    done
    wait
    
    for db in "${databases[@]}"; do 
        create_database $db
    done
    wait

    # 创建表任务
    declare -a create_queries=()

    for ((i=1; i<="$batch_size"; i++)); do
        local table_name="table$i"
        create_query=$(generate_create_query "$table_name")
        create_queries+=("$create_query")
    done
    wait
    
    start_time=$(date +%s)  # 记录开始时间

    # 执行创建表任务
    for query in "${create_queries[@]}"; do
        for db in "${databases[@]}"; do 
            create_table "$db" "$query" &
        done
        wait
    done
    wait  # 等待创建表任务完成

    end_time=$(date +%s)  # 记录结束时间
    duration=$((end_time - start_time))  # 计算时间差

    echo "总耗时：$duration 秒"
    for db in "${databases[@]}"; do 
        drop_database $db
    done
    wait
}
main