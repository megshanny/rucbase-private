/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context 上下文信息
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    
    
    // 获取包含指定记录的页面句柄
    RmPageHandle temp = fetch_page_handle(rid.page_no);
    
    // 检查记录槽位是否被占用；如果未被占用，则抛出记录未找到的异常
    if(!Bitmap::is_set(temp.bitmap, rid.slot_no)){
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    // 将记录数据从页面槽位复制到 record 对象
    char *slot = temp.get_slot(rid.slot_no);
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size);
    memcpy(record->data, slot, file_hdr_.record_size);
    
    // 设置记录大小并返回记录指针
    record->size = file_hdr_.record_size;
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context 上下文信息
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 创建一个新的页面句柄
    RmPageHandle temp = create_page_handle();
    
    // 在页面句柄中找到空闲的槽位
    int slot_no = Bitmap::first_bit(false, temp.bitmap, file_hdr_.num_records_per_page);
    
    // 将找到的槽位设置为占用
    Bitmap::set(temp.bitmap, slot_no);
    
    // 增加页面中记录的数量
    temp.page_hdr->num_records++;
    
    // 如果页面已满，更新文件头的第一个空闲页面号
    if(temp.page_hdr->num_records == file_hdr_.num_records_per_page){
        file_hdr_.first_free_page_no = temp.page_hdr->next_free_page_no;
    }
    
    // 将数据复制到空闲的槽位
    char *slot = temp.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // 返回新记录的位置信息（页面号和槽号）
    return Rid{temp.page->get_page_id().page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // 如果指定页面号超过已有页面数量，创建一个新页面句柄
    if(rid.page_no < file_hdr_.num_pages){
        create_new_page_handle();
    }
    
    // 获取指定页面的句柄
    RmPageHandle temp = fetch_page_handle(rid.page_no);
    
    // 将指定的槽位设置为占用
    Bitmap::set(temp.bitmap, rid.slot_no);
    
    // 增加页面中记录的数量
    temp.page_hdr->num_records++;
    
    // 如果页面已满，更新文件头的第一个空闲页面号
    if(temp.page_hdr->num_records == file_hdr_.num_records_per_page){
        file_hdr_.first_free_page_no = temp.page_hdr->next_free_page_no;
    }
    
    // 将数据复制到指定的槽位
    char *slot = temp.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // 解除页面的固定，并将其标记为已修改
    buffer_pool_manager_->unpin_page(temp.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context 上下文信息
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 获取包含指定记录的页面句柄
    RmPageHandle temp = fetch_page_handle(rid.page_no);
    
    // 检查记录槽位是否被占用；如果未被占用，则抛出记录未找到的异常
    if(!Bitmap::is_set(temp.bitmap, rid.slot_no)){
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    // 如果页面已满，更新文件头的第一个空闲页面号
    if(temp.page_hdr->num_records == file_hdr_.num_records_per_page){
        release_page_handle(temp);
    }
    
    // 重置槽位，删除记录
    Bitmap::reset(temp.bitmap, rid.slot_no);
    
    // 减少页面中的记录数量
    temp.page_hdr->num_records--;
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context 上下文信息
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 获取包含指定记录的页面句柄
    RmPageHandle temp = fetch_page_handle(rid.page_no);
    
    // 检查记录槽位是否被占用；如果未被占用，则抛出记录未找到的异常
    if(!Bitmap::is_set(temp.bitmap, rid.slot_no)){
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    // 获取指定槽位并更新记录数据
    char *slot = temp.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
}

/**
 * 辅助函数：获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // 检查页面号是否合法，如果非法则抛出页面不存在异常
    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError("??", page_no);
    }
    
    // 使用缓冲池获取指定页面并生成页面句柄返回
    PageId page_id;
    page_id.fd = fd_;
    page_id.page_no = page_no;
    Page* page = buffer_pool_manager_->fetch_page(page_id);
    
    return RmPageHandle(&file_hdr_, page);
}

/**
 * 辅助函数：创建一个新的页面句柄
 * @return {RmPageHandle} 新的页面句柄
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // 创建新的页面ID并使用缓冲池分配一个新页面
    PageId *page_id = new PageId;
    page_id->fd = fd_;
    Page* page = buffer_pool_manager_->new_page(page_id);
    
    // 初始化新页面的句柄和相关信息
    RmPageHandle temp = RmPageHandle(&file_hdr_, page);
    temp.page_hdr->num_records = 0;
    temp.page_hdr->next_free_page_no = RM_NO_PAGE;
    Bitmap::init(temp.bitmap, file_hdr_.bitmap_size);
    
    // 更新文件头中的页面数量和第一个空闲页面号
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = page->get_page_id().page_no;
    
    return temp;
}

/**
 * @brief 创建或获取一个空闲的页面句柄
 *
 * @return RmPageHandle 返回生成的空闲页面句柄
 * @note 固定页面，记得在外部解锁！
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // 如果没有空闲页面，则创建一个新的页面句柄
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        return create_new_page_handle();
    } else {
        // 否则，获取第一个空闲页面
        return fetch_page_handle(file_hdr_.first_free_page_no);
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // 当页面从已满变成未满时，更新页面头的下一个空闲页面号
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    
    // 更新文件头的第一个空闲页面号
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}
