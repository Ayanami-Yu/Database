////
// @file block.cc
// @brief
// 实现block
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <algorithm>
#include <cmath>
#include <db/block.h>
#include <db/record.h>
#include <db/table.h>

namespace db {

DataBlock::RecordIterator::RecordIterator()
    : block(nullptr)
    , index(0)
{}
DataBlock::RecordIterator::~RecordIterator() {}
DataBlock::RecordIterator::RecordIterator(const RecordIterator &other)
    : block(other.block)
    , record(other.record)
    , index(other.index)
{}

DataBlock::RecordIterator &DataBlock::RecordIterator::operator++()
{
    if (block == nullptr || block->getSlots() == 0) return *this;
    index = (++index) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return *this;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return *this;
}
DataBlock::RecordIterator DataBlock::RecordIterator::operator++(int)
{
    RecordIterator tmp(*this);
    if (block == nullptr || block->getSlots() == 0) return tmp;
    index = (++index) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return tmp;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return tmp;
}
DataBlock::RecordIterator &DataBlock::RecordIterator::operator--()
{
    if (block == nullptr || block->getSlots() == 0) return *this;
    index = (index + block->getSlots()) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return *this;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return *this;
}
DataBlock::RecordIterator DataBlock::RecordIterator::operator--(int)
{
    RecordIterator tmp(*this);
    if (block == nullptr || block->getSlots() == 0) return tmp;
    index = (index + block->getSlots()) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return tmp;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return tmp;
}
Record *DataBlock::RecordIterator::operator->() { return &record; }
DataBlock::RecordIterator &DataBlock::RecordIterator::operator+=(int step)
{
    if (block == nullptr || block->getSlots() == 0) return *this;
    index = (index + step) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return *this;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return *this;
}
DataBlock::RecordIterator &DataBlock::RecordIterator::operator-=(int step)
{
    if (block == nullptr || block->getSlots() == 0) return *this;
    index = (index + step + block->getSlots()) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return *this;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return *this;
}

void SuperBlock::clear(unsigned short spaceid)
{
    // 清buffer
    ::memset(buffer_, 0, SUPER_SIZE);
    SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);

    // 设置magic number
    header->magic = MAGIC_NUMBER;
    // 设定spaceid
    setSpaceid(spaceid);
    // 设定类型
    setType(BLOCK_TYPE_SUPER);
    // 设定时戳
    setTimeStamp();
    // 设定数据块
    setFirst(0);
    // 设定maxid
    setMaxid(0);
    // 设定self
    setSelf();
    // 设定空闲块，缺省从1开始
    setIdle(0);
    // 设定记录数目
    setRecords(0);
    // 设定数据块个数
    setDataCounts(0);
    // 设定空闲块个数
    setIdleCounts(0);
    // 设定空闲空间
    setFreeSpace(sizeof(SuperHeader));
    // 设置checksum
    setChecksum();
}

void MetaBlock::clear(
    unsigned short spaceid,
    unsigned int self,
    unsigned short type)
{
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);

    // 设定magic
    header->magic = MAGIC_NUMBER;
    // 设定spaceid
    setSpaceid(spaceid);
    // 设定类型
    setType(type);
    // 设定空闲块
    setNext(0);
    // 设置本块id
    setSelf(self);
    // 设定时戳
    setTimeStamp();
    // 设定slots
    setSlots(0);
    // 设定freesize
    setFreeSize(BLOCK_SIZE - sizeof(MetaHeader) - sizeof(Trailer));
    // 设定freespace
    setFreeSpace(sizeof(MetaHeader));
    // 设定校验和
    setChecksum();
}

// TODO: 如果record非full，直接分配，不考虑slot
std::pair<unsigned char *, bool>
MetaBlock::allocate(unsigned short space, unsigned short index)
{
    bool need_reorder = false;
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
    space = ALIGN_TO_SIZE(space); // 先将需要空间数对齐8B

    // 计算需要分配的空间，需要考虑到分配Slot的问题
    unsigned short demand_space = space;
    unsigned short freesize = getFreeSize(); // block当前的剩余空间
    unsigned short current_trailersize = getTrailerSize();
    unsigned short demand_trailersize =
        (getSlots() + 1) * sizeof(Slot) + sizeof(int);
    if (current_trailersize < demand_trailersize)
        demand_space += ALIGN_TO_SIZE(sizeof(Slot)); // 需要的空间数目

    // 该block空间不够
    if (freesize < demand_space)
        return std::pair<unsigned char *, bool>(nullptr, false);

    // 如果freespace空间不够，先回收删除的记录
    unsigned short freespacesize = getFreespaceSize();
    // freespace的空间要减去要分配的slot的空间
    if (current_trailersize < demand_trailersize)
        freespacesize -= ALIGN_TO_SIZE(sizeof(Slot));
    // NOTE: 这里这里没法reorder，才分配还未填充记录
    if (freespacesize < demand_space) {
        shrink();
        need_reorder = true;
    }

    // 从freespace分配空间
    unsigned char *ret = buffer_ + getFreeSpace();

    // 增加slots计数
    unsigned short old = getSlots();
    unsigned short total = std::min<unsigned short>(old, index);
    setSlots(old + 1);
    // 在slots[]顶部增加一个条目
    Slot *new_position = getSlotsPointer();
    for (unsigned short i = 0; i < total; ++i, ++new_position)
        *new_position = *(new_position + 1);
    new_position = getSlotsPointer() + index;
    new_position->offset = htobe16(getFreeSpace());
    new_position->length = htobe16(space);

    // 设定空闲空间大小
    setFreeSize(getFreeSize() - demand_space);
    // 设定freespace偏移量
    setFreeSpace(getFreeSpace() + space);

    return std::pair<unsigned char *, bool>(ret, need_reorder);
}

// TODO: 需要考虑record非full的情况
void MetaBlock::deallocate(unsigned short index)
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);

    // 计算需要删除的记录的槽位
    Slot *pslot = reinterpret_cast<Slot *>(
        buffer_ + BLOCK_SIZE - sizeof(int) -
        sizeof(Slot) * (getSlots() - index));
    Slot slot;
    slot.offset = be16toh(pslot->offset);
    slot.length = be16toh(pslot->length);

    // 设置tombstone
    Record record;
    unsigned char *space = buffer_ + slot.offset;
    record.attach(space, 8); // 只使用8个字节
    record.die();

    // 挤压slots[]
    for (unsigned short i = index; i > 0; --i) {
        Slot *from = pslot;
        --from;
        pslot->offset = from->offset;
        pslot->length = from->length;
        pslot = from;
    }

    // 回收slots[]空间
    unsigned short previous_trailersize = getTrailerSize();
    setSlots(getSlots() - 1);
    unsigned short current_trailersize = getTrailerSize();
    // 要把slots[]回收的空间加回来
    if (previous_trailersize > current_trailersize)
        slot.length += previous_trailersize - current_trailersize;
    // 修改freesize
    setFreeSize(getFreeSize() + slot.length);
}

void MetaBlock::shrink()
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
    Slot *slots = getSlotsPointer();

    // 按照偏移量重新排序slots[]函数
    struct OffsetSort
    {
        bool operator()(const Slot &x, const Slot &y)
        {
            return be16toh(x.offset) < be16toh(y.offset);
        }
    };
    OffsetSort osort;
    std::sort(slots, slots + getSlots(), osort);

    // 枚举所有record，然后向前移动
    unsigned short offset = sizeof(MetaHeader);
    unsigned short space = 0;
    for (unsigned short i = 0; i < getSlots(); ++i) {
        unsigned short len = be16toh((slots + i)->length);
        unsigned short off = be16toh((slots + i)->offset);
        if (offset < off) memmove(buffer_ + offset, buffer_ + off, len);
        (slots + i)->offset = htobe16(offset);
        offset += len;
        space += len;
    }

    // 设定freespace
    setFreeSpace(offset);
    // 计算freesize
    setFreeSize(BLOCK_SIZE - sizeof(MetaHeader) - getTrailerSize() - space);
}

std::pair<unsigned short, bool>
DataBlock::splitPosition(size_t space, unsigned short index)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;
    static const unsigned short BlockHalf =
        (BLOCK_SIZE - sizeof(DataHeader) - 8) / 2; // 一半的大小

    // 枚举所有记录
    unsigned short count = getSlots();
    size_t half = 0;
    Slot *slots = getSlotsPointer();
    bool included = false;
    unsigned short i;
    for (i = 0; i < count; ++i) {
        // 如果是index，则将需要插入的记录空间算在内
        if (i == index) {
            // 这里的计算并不精确，没有准确考虑slot的大小，但只算一半没有太大的误差。
            half += ALIGN_TO_SIZE(space) + sizeof(Slot);
            if (half > BlockHalf)
                break;
            else
                included = true;
        }

        // fallthrough, i != index
        half += be16toh(slots[i].length);
        if (half > BlockHalf) break;
    }
    return std::pair<unsigned short, bool>(i, included);
}

unsigned short DataBlock::requireLength(std::vector<struct iovec> &iov)
{
    size_t length = ALIGN_TO_SIZE(Record::size(iov)); // 对齐8B后的长度
    size_t trailer =
        ALIGN_TO_SIZE((getSlots() + 1) * sizeof(Slot) + sizeof(unsigned int)) -
        ALIGN_TO_SIZE(
            getSlots() * sizeof(Slot) +
            sizeof(unsigned int)); // trailer新增部分
    return (unsigned short) (length + trailer);
}

unsigned short DataBlock::searchRecord(void *buf, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);

    // 获取key位置
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;

    // 调用数据类型的搜索
    return info->fields[key].type->search(buffer_, key, buf, len);
}

std::pair<bool, unsigned short>
DataBlock::insertRecord(std::vector<struct iovec> &iov)
{
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;
    DataType *type = info->fields[key].type;

    // 先确定插入位置
    unsigned short index =
        type->search(buffer_, key, iov[key].iov_base, iov[key].iov_len);

    // 比较key
    Record record;
    if (index < getSlots()) {
        Slot *slots = getSlotsPointer();
        record.attach(
            buffer_ + be16toh(slots[index].offset),
            be16toh(slots[index].length));
        unsigned char *pkey;
        unsigned int len;
        record.refByIndex(&pkey, &len, key);
        if (memcmp(pkey, iov[key].iov_base, len) == 0) // key相等不能插入
            return std::pair<bool, unsigned short>(false, -1);
    }

    // 如果block空间足够，插入
    if (getFreeSize() < requireLength(iov))
        return std::pair<bool, unsigned short>(false, index);

    // 分配空间
    unsigned short actlen = (unsigned short) Record::size(iov);
    std::pair<unsigned char *, bool> alloc_ret = allocate(actlen, index);
    // 填写记录
    record.attach(alloc_ret.first, actlen);
    unsigned char header = 0;
    record.set(iov, &header);
    // 重新排序
    if (alloc_ret.second) reorder(type, key);

    return std::pair<bool, unsigned short>(true, index);
}

std::pair<unsigned int, bool> DataBlock::split(
    unsigned short insertPos,
    std::vector<struct iovec> &iov)
{
    // 分裂 block
    std::pair<unsigned short, bool> splitPos =
        splitPosition(Record::size(iov), insertPos);

    // 先分配一个 block
    DataBlock next;
    next.setTable(table_);
    unsigned int blkid = table_->allocate();
    BufDesp *bd = kBuffer.borrow(table_->name_.c_str(), blkid);
    next.attach(bd->buffer);

    // 移动记录到新的 block 上
    while (getSlots() > splitPos.first) {
        Record record;
        refslots(splitPos.first, record);
        next.copyRecord(record);
        deallocate(splitPos.first);
    }
    kBuffer.releaseBuf(bd);

    // 返回应插在旧还是新 block
    bool included = splitPos.second ? true : false;
    return std::make_pair(blkid, included);
}

bool DataBlock::updateRecord(std::vector<struct iovec>& iov)
{
    if (!removeRecord(iov))  // 记录不存在        
        return false;
    
    std::pair<bool, unsigned short> pret = insertRecord(iov);

    // 保留分裂逻辑，
    // 因为可能有变长记录导致删完再插空间仍不足
    if (!pret.first && pret.second != -1) {  // Block 空间不足       
        std::pair<unsigned int, bool> splitRet = split(pret.second, iov);
        DataBlock next;
        BufDesp *bd = kBuffer.borrow(table_->name_.c_str(), splitRet.first);
        next.attach(bd->buffer);
        next.setTable(table_);

        if (splitRet.second) insertRecord(iov);
        else next.insertRecord(iov);

        // 维护数据链
        next.setNext(getNext());
        setNext(next.getSelf());
        bd->relref();

        // 维护超块头部中的记录数目
        SuperBlock super;
        bd = kBuffer.borrow(table_->name_.c_str(), 0);
        super.attach(bd->buffer);
        super.setRecords(super.getRecords() + 1);
        bd->relref();
    }
    return true;
}

bool DataBlock::removeRecord(std::vector<struct iovec>& iov) 
{ 
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;
    DataType *type = info->fields[key].type;

    // 确定该记录对应的 slot 下标
    unsigned short index =
        type->search(buffer_, key, iov[key].iov_base, iov[key].iov_len);
    if (index >= getSlots()) return false; // 记录不存在

    // 设置记录的 tombstone，挤压 slots
    // 修改 slots 数目，freesize 加回删除的 slot
    deallocate(index);
    
    return true; 
}

int DataBlock::search(
    void *keybuf,
    unsigned int len,
    std::vector<struct iovec> &iov)
{
    SuperBlock super;
    BufDesp *bd = kBuffer.borrow(table_->name_.c_str(), 0);
    super.attach(bd->buffer);

    std::stack<unsigned int> stk; // 存 blockid
    stk.push(super.getRoot());
    kBuffer.releaseBuf(bd); // 释放超块

    DataType *int_type = findDataType("INT");
    while (!stk.empty()) {
        unsigned int blockid = stk.top();
        stk.pop();

        DataBlock data;
        bd = kBuffer.borrow(table_->name_.c_str(), blockid);
        data.attach(bd->buffer);
        data.setTable(table_);
        Slot *slots = data.getSlotsPointer();

        unsigned short ret = data.searchRecord(keybuf, len);
        if (data.getType() == BLOCK_TYPE_DATA) { // 叶节点
            if (ret >= data.getSlots()) { // 记录不存在
                kBuffer.releaseBuf(bd);
                return EFAULT;
            }   
            getRecord(data.buffer_, slots, ret, iov);

            // ret == 0 时仍可能记录不存在
            if (memcmp(keybuf, iov[0].iov_base, iov[0].iov_len) != 0) {
                kBuffer.releaseBuf(bd);
                return EFAULT;
            } else {
                kBuffer.releaseBuf(bd);
                return S_OK;
            }
        } else { // BLOCK_TYPE_INDEX
            if (ret >= data.getSlots()) { 
                getRecord(data.buffer_, slots, data.getSlots() - 1, iov);
                int_type->betoh(iov[1].iov_base);
                stk.push(*(unsigned int *) iov[1].iov_base);
            } else {
                getRecord(data.buffer_, slots, ret, iov);

                // 若相等则为键的右侧指针，否则为左侧
                if (memcmp(keybuf, iov[0].iov_base, iov[0].iov_len) == 0) {
                    int_type->betoh(iov[1].iov_base);
                    stk.push(*(unsigned int *) iov[1].iov_base);
                } else if (ret > 0) {
                    getRecord(data.buffer_, slots, ret - 1, iov);
                    int_type->betoh(iov[1].iov_base);
                    stk.push(*(unsigned int *) iov[1].iov_base);
                } else {
                    stk.push(data.getNext()); // 最左侧指针
                }
            }
        }
    }
    return EFAULT;
}

void DataBlock::attachBuffer(struct BufDesp *bd, unsigned int blockid)
{
    bd = kBuffer.borrow(table_->name_.c_str(), blockid);
    attach(bd->buffer);
}

int DataBlock::insert(std::vector<struct iovec> &iov) 
{
    SuperBlock super;
    BufDesp *bd, *bd2 = nullptr, *bd3 = nullptr;
    bd = kBuffer.borrow(table_->name_.c_str(), 0);
    super.attach(bd->buffer);

    std::stack<unsigned int> stk; // 存 blockid
    stk.push(super.getRoot());
    kBuffer.releaseBuf(bd); // 释放超块

    unsigned int blockid;
    bool needToSplit = false; // 用于当前节点判断是否需分裂后再次插入

    // tmp 用于检验记录是否已存在及获取记录
    // iov 保存了待插入记录所以不能被破坏
    std::vector<struct iovec> tmp(2);
    std::vector<struct iovec> rec; // 上一节点要插入的记录
    std::pair<unsigned int, bool> splitRet;
    std::pair<void *, size_t> recordBuf;
    std::pair<bool, unsigned int> pret;
    DataType *int_type = findDataType("INT");

    DataBlock data, next, parent, root; // 复用时无需再 setTable
    data.setTable(table_);
    next.setTable(table_);
    parent.setTable(table_);
    root.setTable(table_);

    while (!stk.empty()) {
        blockid = stk.top();
        data.attachBuffer(bd, blockid);
        Slot *slots = data.getSlotsPointer();
        unsigned short ret = data.searchRecord(iov[0].iov_base, iov[0].iov_len);  

        if (data.getType() == BLOCK_TYPE_DATA) { // 叶节点            
            stk.pop(); // 准备向上回溯   
            if (ret > 0 && ret < data.getSlots()) { // 记录已存在
                kBuffer.releaseBuf(bd);
                return EFAULT;
            }
            getRecord(data.buffer_, slots, ret, tmp);

            // ret == 0 时记录仍可能已存在
            if (memcmp(iov[0].iov_base, tmp[0].iov_base, iov[0].iov_len) == 0) {
                kBuffer.releaseBuf(bd);
                return EFAULT;
            }
            pret = data.insertRecord(iov);
            if (!pret.first && pret.second != -1) { // Block 空间不足
                splitRet = data.split(pret.second, iov);
                next.attachBuffer(bd2, splitRet.first);

                if (splitRet.second) data.insertRecord(iov);
                else next.insertRecord(iov);

                recordBuf = next.getRecordBuf(0); // 获取新 block 的最小键
                rec = {
                    {recordBuf.first, recordBuf.second},
                    {next.getSelfBuf(), sizeof(unsigned int)}}; // 都为网络字节序
                kBuffer.releaseBuf(bd2);

                blockid = stk.top();
                parent.attachBuffer(bd2, blockid);
                pret = parent.insertRecord(rec);
                if (!pret.first && pret.second != -1) // 父节点需要分裂
                    needToSplit = true;
                
                kBuffer.releaseBuf(bd2);                                
            }
            kBuffer.releaseBuf(bd);

            while (!stk.empty()) { // 开始回溯
                blockid = stk.top();
                stk.pop();

                if (needToSplit) {
                    needToSplit = false;
                    data.attachBuffer(bd, blockid);
                    splitRet = data.split(pret.second, rec);
                    next.attachBuffer(bd2, splitRet.first);

                    if (splitRet.second)
                        data.insertRecord(rec);
                    else
                        next.insertRecord(rec);

                    recordBuf = next.getRecordBuf(0);
                    rec.clear();
                    rec = {
                        {recordBuf.first, recordBuf.second},
                        {next.getSelfBuf(), sizeof(unsigned int)}};
                    kBuffer.releaseBuf(bd);
                    kBuffer.releaseBuf(bd2);

                    // 尝试给父节点插入中位键
                    blockid = stk.top();
                    parent.attachBuffer(bd, blockid);
                    pret = parent.insertRecord(rec);
                    if (!pret.first && pret.second != -1) needToSplit = true;

                    kBuffer.releaseBuf(bd);
                }
            }
            if (needToSplit) { // 根节点需要分裂再插入
                bd = kBuffer.borrow(table_->name_.c_str(), 0); // 获取超块
                super.attach(bd->buffer);

                blockid = super.getRoot();
                data.attachBuffer(bd2, blockid); // 获取根
                splitRet = data.split(pret.second, rec);
                next.attachBuffer(bd3, splitRet.first); // 获取新 block

                if (splitRet.second)
                    data.insertRecord(rec);
                else
                    next.insertRecord(rec);
                kBuffer.releaseBuf(bd2);
                kBuffer.releaseBuf(bd3);

                recordBuf = next.getRecordBuf(0);
                rec.clear();
                rec = {
                    {recordBuf.first, recordBuf.second},
                    {next.getSelfBuf(), sizeof(unsigned int)}};

                unsigned int rootId = table_->allocate(); // 申请新 block 作为根
                root.attachBuffer(bd2, rootId);
                root.insertRecord(rec);
                root.setNext(data.getSelf());
                super.setRoot(rootId); // 维护超块中的根 blockid

                kBuffer.releaseBuf(bd);
                kBuffer.releaseBuf(bd2);
            }
            return S_OK;
        } else { // BLOCK_TYPE_INDEX
            if (ret >= data.getSlots()) {
                getRecord(data.buffer_, slots, data.getSlots() - 1, tmp);
                int_type->betoh(tmp[1].iov_base);
                stk.push(*(unsigned int *) tmp[1].iov_base);
            } else {
                getRecord(data.buffer_, slots, ret, tmp);

                // 若相等则为键的右侧指针，否则为左侧
                if (memcmp(tmp[0].iov_base, iov[0].iov_base, iov[0].iov_len) == 0) {
                    int_type->betoh(tmp[1].iov_base);
                    stk.push(*(unsigned int *) tmp[1].iov_base);
                } else if (ret > 0) {
                    getRecord(data.buffer_, slots, ret - 1, iov);
                    int_type->betoh(tmp[1].iov_base);
                    stk.push(*(unsigned int *) tmp[1].iov_base);
                } else {
                    stk.push(data.getNext()); // 最左侧指针
                }
            }
            kBuffer.releaseBuf(bd);
        }
    }
    return EFAULT;
}

int DataBlock::remove(std::vector<struct iovec> &iov)
{
    SuperBlock super;
    BufDesp *bd = kBuffer.borrow(table_->name_.c_str(), 0);
    super.attach(bd->buffer);

    std::stack<unsigned int> stk; // 存 blockid
    stk.push(super.getRoot());
    kBuffer.releaseBuf(bd); // 释放超块

    DataType *int_type = findDataType("INT");
    std::vector<struct iovec> tmp(2);
    while (!stk.empty()) {
        unsigned int blockid = stk.top();
        stk.pop();

        DataBlock data;
        bd = kBuffer.borrow(table_->name_.c_str(), blockid);
        data.attach(bd->buffer);
        data.setTable(table_);
        Slot *slots = data.getSlotsPointer();

        unsigned short ret = data.searchRecord(iov[0].iov_base, iov[0].iov_len);
        if (data.getType() == BLOCK_TYPE_DATA) { // 叶节点
            if (ret >= data.getSlots()) {        // 记录不存在
                kBuffer.releaseBuf(bd);
                return EFAULT;
            }
            getRecord(data.buffer_, slots, ret, tmp);

            // ret == 0 时仍可能记录不存在
            if (memcmp(iov[0].iov_base, tmp[0].iov_base, iov[0].iov_len) != 0) {
                kBuffer.releaseBuf(bd);
                return EFAULT;
            } else { // 找到待删除记录
                data.removeRecord(iov);
                if (data.isUnderflow()) {}

                kBuffer.releaseBuf(bd);
                return S_OK;
            }
        } else { // BLOCK_TYPE_INDEX
            if (ret >= data.getSlots()) {
                getRecord(data.buffer_, slots, data.getSlots() - 1, tmp);
                int_type->betoh(tmp[1].iov_base);
                stk.push(*(unsigned int *) tmp[1].iov_base);
            } else {
                getRecord(data.buffer_, slots, ret, tmp);

                // 若相等则为键的右侧指针，否则为左侧
                if (memcmp(iov[0].iov_base, tmp[0].iov_base, iov[0].iov_len) ==
                    0) {
                    int_type->betoh(tmp[1].iov_base);
                    stk.push(*(unsigned int *) tmp[1].iov_base);
                } else if (ret > 0) {
                    getRecord(data.buffer_, slots, ret - 1, tmp);
                    int_type->betoh(tmp[1].iov_base);
                    stk.push(*(unsigned int *) tmp[1].iov_base);
                } else {
                    stk.push(data.getNext()); // 最左侧指针
                }
            }
        }
    }
    return EFAULT;
}

int DataBlock::update(std::vector<struct iovec> &iov) 
{
    return S_OK;
}

bool DataBlock::copyRecord(Record &record)
{
    // 判断剩余空间是否足够
    size_t blen = getFreespaceSize(); // 该block的富余空间
    unsigned short actlen = (unsigned short) record.allocLength();
    unsigned short trailerlen =
        ALIGN_TO_SIZE((getSlots() + 1) * sizeof(Slot) + sizeof(unsigned int)) -
        ALIGN_TO_SIZE(getSlots() * sizeof(Slot) + sizeof(unsigned int));
    if (blen < actlen + trailerlen) return false;

    // 分配空间，然后copy
    std::pair<unsigned char *, bool> alloc_ret = allocate(actlen, getSlots());
    memcpy(alloc_ret.first, record.buffer_, actlen);

#if 0
    // 重新排序，最后才重拍？
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;
    DataType *type = info->fields[key].type;
    reorder(type, key); // 最后才重排？
#endif
    return true;
}

DataBlock::RecordIterator DataBlock::beginrecord()
{
    RecordIterator ri;
    ri.block = this;
    ri.index = 0;

    if (getSlots()) {
        Slot *slots = getSlotsPointer();
        ri.record.attach(
            buffer_ + be16toh(slots[0].offset), be16toh(slots[0].length));
    }
    return ri;
}
DataBlock::RecordIterator DataBlock::endrecord()
{
    RecordIterator ri;
    ri.block = this;
    ri.index = getSlots();
    return ri;
}

} // namespace db
