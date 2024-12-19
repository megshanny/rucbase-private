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
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    /*
        从 rec_cols 列表中查找与 target 匹配的列（根据表名和列名）。
        如果找到了，返回该列在 rec_cols 中的迭代器；
        如果没有找到，则抛出 ColumnNotFoundError 异常。
    */
    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) 
    {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) 
        {
            return col.tab_name == target.tab_name && col.name == target.col_name; 
        });
        if (pos == rec_cols.end()) 
        {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }
   
    bool condCheck(const RmRecord *l_record, const std::vector<Condition>& conds_, const std::vector<ColMeta>& cols_) 
    {
        char *l_val, *r_val;

        for (auto &cond : conds_) // 条件判断
        {  
            CompOp op = cond.op;
            int cmp;

            auto l_col = get_col(cols_, cond.lhs_col);  // 左列元数据
            l_val = l_record->data + l_col->offset;      // 确定左数据起点

            if (cond.is_rhs_val)    //如果右边是值
            { 
                r_val = cond.rhs_val.raw.get()->data; 
                cmp = ix_compare(l_val, r_val, cond.rhs_val.type, l_col->len);
            } 
            else                    // 如果右边是列
            {  
                auto r_col = get_col(cols_, cond.rhs_col); // 右列元数据
                r_val = l_record->data + r_col->offset; // 确定右数据起点
                cmp = ix_compare(l_val, r_val, r_col->type, l_col->len); 
            }
            if (!op_compare(op, cmp)) // 比较结果不符合条件
                return false;
        }
        return true;
    }
    static bool op_compare(CompOp op, int cmp) 
    {
        if (op == OP_EQ) {
            return cmp == 0;
        } else if (op == OP_NE) {
            return cmp != 0;
        } else if (op == OP_LT) {
            return cmp < 0;
        } else if (op == OP_GT) {
            return cmp > 0;
        } else if (op == OP_LE) {
            return cmp <= 0;
        } else if (op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected field type");
        }
    }
};