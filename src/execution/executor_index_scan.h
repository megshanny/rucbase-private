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

#include <set>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表名称
    TabMeta tab_;                       // 表的元数据
    std::vector<Condition> conds_;      // 扫描条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // 需要读取的字段
    size_t len_;                        // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;  // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        
        // 将左边的列调整为索引列
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };
        for (auto &cond : conds_) 
        {
            if (cond.lhs_col.tab_name != tab_name_)  
            {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override 
    {
        // index is available, scan index
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        Iid lower = ih->leaf_begin();
        Iid upper = ih->leaf_end();
        for (auto &index_col : index_col_names_) 
        {
            for (auto &cond : fed_conds_) 
            {
                if (cond.is_rhs_val && cond.op != OP_NE && cond.lhs_col.col_name == index_col) //
                {
                    // 获得索引列的值
                    int offset = 0;
                    char *key = new char[index_meta_.col_tot_len]; // 拼接得到索引列的值
                    for (size_t i = 0; i < index_meta_.col_num; ++i) 
                    {
                        auto &cond = fed_conds_[i];
                        auto &col = index_meta_.cols[i];
                        memcpy(key + offset, cond.rhs_val.raw->data, col.len);
                        offset += col.len;
                    }
                    // 根据不同的条件，调整lower和upper
                    if (cond.op == OP_EQ) {
                        lower = ih->lower_bound(key);
                        upper = ih->upper_bound(key);
                    } else if (cond.op == OP_LT) {
                        upper = ih->lower_bound(key);
                    } else if (cond.op == OP_GT) {
                        lower = ih->upper_bound(key);
                    } else if (cond.op == OP_LE) {
                        upper = ih->upper_bound(key);
                    } else if (cond.op == OP_GE) {
                        lower = ih->lower_bound(key);
                    } else {
                        throw InternalError("Unexpected field type");
                    }
                    break;  // only one index column is allowed
                }
            }
        }

        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        // Get the first record
        while (!scan_->is_end()) 
        {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if(condCheck(rec.get(), fed_conds_, cols_)) break;
            scan_->next();
        }
    }

    void nextTuple() override {
        // 扫描到下一个满足条件的记录,赋rid_,中止循环
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            if (condCheck(fh_->get_record(rid_, context_).get(), fed_conds_, cols_)) break;
        }
    }

    bool is_end() const override { return scan_->is_end(); }

    std::unique_ptr<RmRecord> Next() override  //获取当前记录
    {
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
};