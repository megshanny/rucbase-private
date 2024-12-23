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

class ProjectionExecutor : public AbstractExecutor 
{
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 提供输入元组数据，即投影的源数据
    std::vector<ColMeta> cols_;                     // 存储需要投影的列的元数据
    size_t len_;                                    // 存储投影后元组的总长度
    std::vector<size_t> sel_idxs_;                  // 投影操作选择的列的索引

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) 
    {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols(); 
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    void beginTuple() override {
        prev_->beginTuple();
    }

    void nextTuple() override {
        prev_->nextTuple();
    }

    std::unique_ptr<RmRecord> Next() override 
    {
        auto &prev_cols = prev_->cols(); 
        auto prev_rec = prev_->Next(); // 从前一个执行器获取下一个元组 prev_rec
        
        auto &proj_cols = cols_; // 获取需要投影的列的元数据
        auto proj_rec = std::make_unique<RmRecord>(len_); //创建一个新的 RmRecord 对象 proj_rec 来存储投影后的记录
        for (size_t i = 0; i < proj_cols.size(); i++) 
        {
            size_t prev_idx = sel_idxs_[i]; // 获取需要投影的列的索引
            auto &prev_col = prev_cols[prev_idx];
            auto &proj_col = proj_cols[i]; 
            memcpy(proj_rec->data + proj_col.offset, prev_rec->data + prev_col.offset, prev_col.len);
        }
        return proj_rec;
    }

    bool is_end() const override { return prev_->is_end(); }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    Rid &rid() override { return _abstract_rid; }
};