/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */

bool check_state(Transaction* txn)
{
    TransactionState txn_stat = txn->get_state();
    if(txn_stat == TransactionState::SHRINKING)
    {
        throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHIRINKING);
        return false;
    }
    else if(txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED)
    {
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    return true;
}

bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) 
{
    std::unique_lock<std::mutex> lock{latch_};
    
    if(!check_state(txn))
    {
        return false;
    }

    // 检查是否已经上过该记录的锁（record只会有SX锁）
    LockDataId rec_lockID = LockDataId(tab_fd,rid,LockDataType::RECORD);
    auto txn_locks = txn->get_lock_set();
    if(txn_locks->find(rec_lockID) != txn_locks->end())
    {
        return true;
    }

    // 检查是否有其他事务的锁，有排他锁则直接abort
    for(auto it: lock_table_[rec_lockID].request_queue_)
    {
        if(it.txn_id_ != txn->get_transaction_id()
            && it.lock_mode_ == LockMode::EXLUCSIVE)
            {
                throw TransactionAbortException(txn->get_transaction_id(),AbortReason::DEADLOCK_PREVENTION);
                return false;
            }
    }

    // 加锁
    // 把锁IDinsert进本事务的锁集
    txn_locks->insert(rec_lockID);
    // 创建新的锁申请
    LockRequest lock_request = LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true; 
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    //更新group_lock_mode_
    if(lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::S)
    {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::S;
    }
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) 
{    
    std::unique_lock<std::mutex> lock{latch_};
    
    if(!check_state(txn))
    {
        return false;
    }

    // 如果有其他事务的锁，直接abort
    // 记录本事务已经有的锁
    LockDataId rec_lockID = LockDataId(tab_fd,rid,LockDataType::RECORD);
    std::list<LockRequest>::iterator txn_rlock_it;
    bool Sharedlock =false;

    for(auto it = lock_table_[rec_lockID].request_queue_.begin();it!=lock_table_[rec_lockID].request_queue_.end();it++)
    {
        if(it->txn_id_ != txn->get_transaction_id())
        {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            return false;
        }
        else
        {
            if(it->lock_mode_ == LockMode::EXLUCSIVE)
            {
                return true;
            }
            else
            {
                txn_rlock_it = it;
                Sharedlock = true;
            }
        }
    }
    // 如果已经有了共享锁，直接升级
    if(Sharedlock)
    {
        txn_rlock_it->lock_mode_ = LockMode::EXLUCSIVE;
        if(lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::X)
        {
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::X;
        }
        return true;
    }
    // 没有锁情况下，加锁  
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_req = LockRequest(txn->get_transaction_id(),LockMode::EXLUCSIVE);
    lock_req.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_req);
    if(lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::X)
    {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::X;
    }
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) 
{
    std::unique_lock<std::mutex> lock{latch_};

    if(!check_state(txn))
    {
        return false;
    }
    
    LockDataId rec_lockID = LockDataId(tab_fd,LockDataType::TABLE);
    bool hasOtherLock = false;
    std::list<LockRequest>::iterator iterIS; 
    bool hasIS = false;
    std::list<LockRequest>::iterator iterIX;
    bool hasIX = false;
    for(auto it = lock_table_[rec_lockID].request_queue_.begin();it!=lock_table_[rec_lockID].request_queue_.end();it++)
    {
        if(it->txn_id_ != txn->get_transaction_id()) //检查是否有其它事务的X IX SIX锁，如果有则abort
        {
            if(it->lock_mode_ == LockMode::EXLUCSIVE 
                || it->lock_mode_ == LockMode::INTENTION_EXCLUSIVE
                || it->lock_mode_ == LockMode::S_IX)
                {
                    throw TransactionAbortException(txn->get_transaction_id(),AbortReason::DEADLOCK_PREVENTION);
                    return false;
                }
        }
        else
        {
            if(it->lock_mode_ == LockMode::INTENTION_SHARED)
            {
                iterIS = it;
                hasIS = true;
            }
            else if(it->lock_mode_ == LockMode::INTENTION_EXCLUSIVE)
            {
                iterIX = it;
                hasIX = true;
            }
            else
            {
                hasOtherLock = true;
            }
        }
    }

    if(hasOtherLock) //如果S X SIX，直接返回
    {
        return true;
    }
    if(hasIS) //如果IS，升级成S
    {
        iterIS->lock_mode_ = LockMode::SHARED;
        if(lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::S){
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::S;
        }
        return true;
    }
    if(hasIX) //如果IX，升级成SIX
    {
        iterIX->lock_mode_ = LockMode::S_IX;
        if(lock_table_[rec_lockID].group_lock_mode_ != GroupLockMode::X)
        {
            //如果是X，不用升级
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::SIX;
        }
        return true;
    }
    // 没有锁，直接加锁
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(),LockMode::SHARED);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if(lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::S)
    {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::S;
    }
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    
    std::unique_lock<std::mutex> lock{latch_};
    
    if(!check_state(txn))
    {
        return false;
    }

    // 检查是否有其他事务的锁，直接abort
    LockDataId rec_lockID = LockDataId(tab_fd,LockDataType::TABLE);
    bool thisLock = false; // IS IX S SIX -> X
    std::list<LockRequest>::iterator weaker_it;
    for(auto it = lock_table_[rec_lockID].request_queue_.begin();it!=lock_table_[rec_lockID].request_queue_.end();it++){
        if(it->txn_id_ != txn->get_transaction_id()){
            throw TransactionAbortException(txn->get_transaction_id(),AbortReason::DEADLOCK_PREVENTION);
            return false;
        }
        else
        {
            if(it->lock_mode_==LockMode::EXLUCSIVE)
            {
                return true;
            }
            else
            {
                thisLock = true;
                weaker_it = it;
            }
        }
    }

    // IS,IX,S,SIX 升级成 X;
    if(thisLock)
    {
        weaker_it->lock_mode_ = LockMode::EXLUCSIVE;
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::X;
        return true;
    }

    // 没有锁，直接加锁
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(),LockMode::EXLUCSIVE);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if(lock_table_[rec_lockID].group_lock_mode_ != GroupLockMode::X){
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::X;
    }
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    
    std::unique_lock<std::mutex> lock{latch_};

    if(!check_state(txn))
    {
        return false;
    }

    LockDataId rec_lockID = LockDataId(tab_fd,LockDataType::TABLE);
    for(auto it = lock_table_[rec_lockID].request_queue_.begin();it!=lock_table_[rec_lockID].request_queue_.end();it++)
    {
        if(it->txn_id_ != txn->get_transaction_id()&& it->lock_mode_ == LockMode::EXLUCSIVE)
        {
            throw TransactionAbortException(txn->get_transaction_id(),AbortReason::DEADLOCK_PREVENTION);
            return false;
        }
        else
        {
            //当前事务有锁，直接返回，不会冲突
            return true;
        }
    }
    
    //没有锁，直接加锁
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(),LockMode::INTENTION_SHARED);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if(lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::IS){
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::IS;
    }
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) 
{
    std::unique_lock<std::mutex> lock{latch_};
    
    if(!check_state(txn))
    {
        return false;
    }

    // 检查是否有其它事务的锁，S X SIX需要abort
    LockDataId rec_lockID = LockDataId(tab_fd,LockDataType::TABLE);
    bool s_found = false;                   // for s upgrade to SIX
    std::list<LockRequest>::iterator s_it;
    bool hasIS = false;                  // for is upgrade to IX
    std::list<LockRequest>::iterator iterIS;

    for(auto it=lock_table_[rec_lockID].request_queue_.begin();it!=lock_table_[rec_lockID].request_queue_.end();it++)
    {
        if(it->txn_id_ != txn->get_transaction_id()
        && (it->lock_mode_ == LockMode::SHARED
            || it->lock_mode_ == LockMode::EXLUCSIVE
            || it->lock_mode_ == LockMode::S_IX))
            {
                throw TransactionAbortException(txn->get_transaction_id(),AbortReason::DEADLOCK_PREVENTION);
                return false;
            }
        else
        {
            if(it->lock_mode_ == LockMode::SHARED)
            {
                s_found = true;
                s_it = it;
            }
            else if(it->lock_mode_ == LockMode::INTENTION_SHARED)
            {
                hasIS = true;
                iterIS = it;
            }
            else
            {
                //X\IX\SIX锁return true
                return true;
            }
        }
    }
    // S锁需要升级成SIX
    if(s_found)
    {
        s_it->lock_mode_ = LockMode::S_IX;
        if(lock_table_[rec_lockID].group_lock_mode_ != GroupLockMode::X)
        {
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::SIX;
        }
        return true;
    }
    // IS锁需要升级成IX
    if(hasIS)
    {
        iterIS->lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
        if(lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::X)
        { 
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::IX;
        }
        return true;
    }

    // 没有锁，直接加锁
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(),LockMode::INTENTION_EXCLUSIVE);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if(lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::X)
    {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::IX;
    }
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) 
{    
    std::unique_lock<std::mutex> lock{latch_};
    // 检查并修改事务状态
    TransactionState txn_stat = txn->get_state();
    if(txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED)
    {
        return false;
    }
    txn->set_state(TransactionState::SHRINKING);

    // 检查txn和data_id是否匹配
    if(txn->get_lock_set()->find(lock_data_id) == txn->get_lock_set()->end()){
        return false;
    }
    // 在全局锁表里删掉txn开的所有锁
    for(auto it=lock_table_[lock_data_id].request_queue_.begin();it!=lock_table_[lock_data_id].request_queue_.end();)
    {
        if(it->txn_id_ == txn->get_transaction_id())
        {
            it = lock_table_[lock_data_id].request_queue_.erase(it);
        }
        else
        {
            it++;
        }
    }
    // 修改lock_table_[lock_data_id]的GroupLockMode
    GroupLockMode new_mode = GroupLockMode::NON_LOCK;
    for(auto it=lock_table_[lock_data_id].request_queue_.begin();it!=lock_table_[lock_data_id].request_queue_.end();it++){
        if(it->granted_){
            switch(it->lock_mode_){
                case LockMode::INTENTION_SHARED:{
                    if(new_mode == GroupLockMode::NON_LOCK){
                        new_mode = GroupLockMode::IS;
                    }
                    break;
                }
                case LockMode::INTENTION_EXCLUSIVE:
                {
                    if(new_mode == GroupLockMode::NON_LOCK || new_mode == GroupLockMode::IS)
                    {
                        new_mode = GroupLockMode::IX;
                    }
                    else if(new_mode == GroupLockMode::S)
                    {
                        new_mode = GroupLockMode::SIX;
                    }
                    break;
                }
                case LockMode::SHARED:
                {
                    if(new_mode == GroupLockMode::NON_LOCK 
                        || new_mode == GroupLockMode::IS){
                        new_mode = GroupLockMode::S;
                    }
                    else if(new_mode == GroupLockMode::IX){
                        new_mode = GroupLockMode::SIX;
                    }
                    break;
                }
                case LockMode::EXLUCSIVE:
                {
                    new_mode = GroupLockMode::X;
                    break;
                }
                case LockMode::S_IX:
                {
                    if(new_mode != GroupLockMode::X){
                        new_mode = GroupLockMode::SIX;
                    }
                    break;
                }
            }
        }
    }
    lock_table_[lock_data_id].group_lock_mode_ = new_mode;
    return true;
}