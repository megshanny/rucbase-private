/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;              // 表的元数据
    std::vector<Condition> conds_;  // 更新条件
    RmFileHandle *fh_;         // 表的文件句柄，用于操作记录
    std::vector<Rid> rids_;    // 记录 ID 列表，表示需要更新的记录
    std::string tab_name_;     // 表名
    std::vector<SetClause> set_clauses_; // 更新的列和对应的值
    SmManager *sm_manager_;    // 数据库管理器，处理文件和索引操作

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override 
    {
        // 遍历每个记录 ID（rids_），并更新对应的记录
        for (auto &rid : rids_) 
        {
            // 获取当前记录的数据
            auto rec = fh_->get_record(rid, context_);

            // 遍历每个更新的列和对应的新值
            for (auto &set_clause : set_clauses_) 
            {
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);// 获取目标列的元数据
                memcpy(rec->data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len); // 将新值复制到目标列位置
            }

            // 删除该记录在索引中的旧条目
            for (auto & index : tab_.indexes) 
            {
                // 获取索引处理句柄
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                
                 // 创建索引键
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) 
                {
                    memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->delete_entry(key, context_->txn_);
            }

            // 更新记录（将修改后的数据写回文件）
            fh_->update_record(rid, rec->data, context_);
            
            // 在索引中插入新的条目
            for (auto & index : tab_.indexes) 
            {
                // 获取索引处理句柄
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                
                // 创建新的索引键
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) 
                {
                    memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->insert_entry(key, rid, context_->txn_);
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    size_t tupleLen() const override { return 0; };
    void beginTuple() override{};
    void nextTuple() override{};
    bool is_end() const override { return true; };
    std::string getType() override { return "UpdateExecutor"; };
    ColMeta get_col_offset(const TabCol &target) override { return ColMeta(); };
};