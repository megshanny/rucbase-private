/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) 
{
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    std::ifstream ifs(DB_META_NAME);
    ifs >> db_;
    ifs.close();

    // 打开所有表文件
    for (auto &entry : db_.tabs_) {
        fhs_.emplace(entry.first, rm_manager_->open_file(entry.first));
    }

    // 打开所有索引文件
    for (auto &entry : db_.tabs_) {
        for (auto &index : entry.second.indexes) { // 遍历表的索引
            ihs_.emplace(ix_manager_->get_index_name(entry.first, index.cols), ix_manager_->open_index(entry.first, index.cols)); 
        }

        for (auto &index : entry.second.indexes) {  // 遍历表的索引
            drop_index(entry.first, index.cols, nullptr); // 删除索引,
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() 
{
    std::ofstream ofs(DB_META_NAME); // 打开文件, 会清空文件, 重新写入, 保存数据库元数据
    ofs << db_; // 将数据库元数据写入文件
    ofs.close(); // 关闭文件

    db_.tabs_.clear();  // 清空数据库中的表
    db_.name_.clear();  // 清空数据库名称

    // 关闭所有文件句柄
    for (auto &entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
    fhs_.clear();

    // 关闭所有索引句柄
    for (auto &entry : ihs_) {
        ix_manager_->close_index(entry.second.get());
    }
    ihs_.clear();

    if (chdir("..") < 0)
    {
        throw UnixError();
    }
    
    flush_meta();
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) 
{
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    //lab4
    context->lock_mgr_->lock_exclusive_on_table(context->txn_,fhs_[tab_name]->GetFd());

    //获取表元数据TabMeta
    TabMeta &tab = db_.get_table(tab_name);
    // 删除记录文件
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name);

    // 删除表的索引
    for (auto &index : tab.indexes) {
        drop_index(tab_name, index.cols, nullptr);
    }
    //最后在数据库元数据文件和记录文件句柄中删除该表信息
    db_.tabs_.erase(tab_name);
    fhs_.erase(tab_name);
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) 
{
    // 1. 获取表的元数据
    // 从数据库中获取指定表的元数据对象（tab_meta），以便后续操作
    auto& tab_meta = db_.get_table(tab_name);

    // 2. 创建并初始化索引元数据对象
    // 索引元数据对象 (index_meta) 包含了关于索引的基本信息
    IndexMeta index_meta = {tab_name};  // 初始化索引元数据，设置表名

    // 3. 获取列的元数据并计算索引相关信息
    // col_meta 是一个存储列元数据的 vector，index_meta.cols 存储了所有索引列的信息
    std::vector<ColMeta>& col_meta = index_meta.cols;
    for (auto& col : col_names) {
        // 通过表的元数据对象获取指定列的元数据
        auto it = tab_meta.get_col(col);
        
        // 将列的元数据添加到索引元数据的列列表中
        col_meta.push_back(*it);
        
        // 更新索引元数据：计算列的总长度和列的数量
        index_meta.col_tot_len += it->len;
        index_meta.col_num++;
    }

    // 4. 锁定表，确保操作的独占性
    // 如果 context 存在，且无法对表进行独占锁定，抛出异常
    context->lock_mgr_->lock_exclusive_on_table(context->txn_,fhs_[tab_name]->GetFd());

    // 5. 调用索引管理器创建索引
    // 使用索引管理器创建索引并将相关列元数据传递给它
    ix_manager_->create_index(tab_name, col_meta);

    // 6. 更新表的索引列表
    // 将创建的索引元数据添加到表的索引列表中
    tab_meta.indexes.push_back(index_meta);

    // 7. 将索引句柄保存在索引句柄映射表中
    // 打开并获取该索引的句柄，然后将其存入索引句柄映射表（ihs_）
    ihs_[ix_manager_->get_index_name(tab_name, col_meta)] = ix_manager_->open_index(tab_name, col_meta);
}


/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // context->lock_mgr_->lock_exclusive_on_table(context->txn_, disk_manager_->get_file_fd(tab_name)); // 锁定表
    // lab4
    context->lock_mgr_->lock_exclusive_on_table(context->txn_,fhs_[tab_name]->GetFd());

    if (!ix_manager_->exists(tab_name, col_names)) { // 如果索引不存在
        throw IndexNotFoundError(tab_name, col_names);
    }

    auto index_name = ix_manager_->get_index_name(tab_name, col_names); // 获取索引名称
    
    ix_manager_->close_index(ihs_[index_name].get());
    ix_manager_->destroy_index(tab_name, col_names);

    TabMeta& tab = db_.get_table(tab_name);
    tab.indexes.erase(tab.get_index_meta(col_names));

    ihs_.erase(index_name);
    flush_meta();

}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) 
{
    
    std::vector<std::string> col_names;
    for (auto& col : cols) {
        col_names.push_back(col.name);
    }

    drop_index(tab_name, col_names, context);

}