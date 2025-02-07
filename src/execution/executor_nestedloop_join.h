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

class NestedLoopJoinExecutor : public AbstractExecutor 
{
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点（需要join的表）
    size_t len_;                               // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                // join后获得的记录的字段

    std::vector<Condition> fed_conds_;  // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left); 
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen(); // 连接后每条记录的总字节数
        cols_ = left_->cols();

        auto right_cols = right_->cols();
        for (auto &col : right_cols) 
        {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end()); // 连接后的字段
        isend = false; 
        fed_conds_ = std::move(conds);
    }

    void beginTuple() override 
    {
        left_->beginTuple();
        right_->beginTuple();
    }

    void nextTuple() override 
    {
         // 遍历右表
        for (; !right_->is_end(); right_->nextTuple()) 
        {
            // 处理左表的移动
            if (left_->is_end())
                left_->beginTuple();
            else // 左表还没匹配完，接着匹配，不然就一直匹配同一个死循环了
                left_->nextTuple();

            // 遍历左表的每一行，并进行连接检查
            for (; !left_->is_end(); left_->nextTuple()) 
            {
                if (condCheck(Next().get(), fed_conds_, cols_)) return;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override 
    { 
        std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(len_);
        std::unique_ptr<RmRecord> l_rec = left_->Next();
        std::unique_ptr<RmRecord> r_rec = right_->Next();
        // 组合成一条record
        memset(record->data, 0, record->size);
        memcpy(record->data, l_rec->data, l_rec->size);
        memcpy(record->data + l_rec->size, r_rec->data, r_rec->size);

        return record;
    }
    
    Rid &rid() override { return _abstract_rid; }
    bool is_end() const override { return left_->is_end(); }
    size_t tupleLen() const override { return len_; };
    std::string getType() override { return "NestedLoopJoinExecutor"; };
    const std::vector<ColMeta> &cols() const override { return cols_; };
};