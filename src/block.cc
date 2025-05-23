// 实现block
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
    if (!pret.first && pret.second != (unsigned short) -1) { // Block 空间不足       
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
    unsigned int keyIdx = info->key;
    DataType *keyType = info->fields[keyIdx].type;

    // 确定该记录对应的 slot 下标
    unsigned short index =
        keyType->search(buffer_, keyIdx, iov[keyIdx].iov_base, iov[keyIdx].iov_len);
    if (index >= getSlots()) return false; // 记录不存在

    // 当 index 处于范围中时仍可能记录不存在
    Slot *slots = getSlotsPointer();
    Record record;
    record.attach(
        buffer_ + be16toh(slots[index].offset), be16toh(slots[index].length));

    size_t keySize = getKeyBytes(keyType);
    std::vector<char> tmpKey(keySize);
    unsigned int tmpKeyLen = (unsigned int) iov[keyIdx].iov_len;
    
    record.getByIndex((char *) &tmpKey[0], &tmpKeyLen, keyIdx);
    if (memcmp(&tmpKey[0], iov[keyIdx].iov_base, iov[keyIdx].iov_len) != 0) return false;

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
    RelationInfo *info = table_->info_;
    unsigned int keyIdx = info->key;
    DataType *keyType = info->fields[keyIdx].type;

    SuperBlock super;
    BufDesp *bd = kBuffer.borrow(table_->name_.c_str(), 0);
    super.attach(bd->buffer);

    // 用于暂存搜索的结果
    size_t keySize = getKeyBytes(keyType);
    std::vector<char> tmpKey(keySize);
    unsigned int tmpVal;
    std::vector<struct iovec> tmp = {
        {&tmpKey[0], keySize}, {&tmpVal, sizeof(unsigned int)}};

    std::stack<unsigned int> stk; // 存 blockid
    stk.push(super.getRoot());
    kBuffer.releaseBuf(bd); // 释放超块

    // blockid 的数据类型是固定的
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
            if (memcmp(keybuf, iov[keyIdx].iov_base, iov[keyIdx].iov_len) != 0) {                
                kBuffer.releaseBuf(bd);
                return EFAULT;
            } else {
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
                if (memcmp(keybuf, tmp[keyIdx].iov_base, tmp[keyIdx].iov_len) == 0) {
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

void DataBlock::attachBuffer(struct BufDesp **bd, unsigned int blockid)
{
    *bd = kBuffer.borrow(table_->name_.c_str(), blockid);
    attach((*bd)->buffer);
}

int DataBlock::insert(std::vector<struct iovec> &iov) 
{
    RelationInfo *info = table_->info_;
    unsigned int keyIdx = info->key;
    DataType *keyType = info->fields[keyIdx].type;
    DataType *int_type = findDataType("INT");

    SuperBlock super;
    BufDesp *bd, *bd2 = nullptr, *bd3 = nullptr;
    bd = kBuffer.borrow(table_->name_.c_str(), 0);
    super.attach(bd->buffer);

    std::stack<unsigned int> stk; // 存 blockid
    stk.push(super.getRoot());
    kBuffer.releaseBuf(bd); // 释放超块

    Record tmpRecord;
    std::vector<char> tmpKeyBuf(iov[keyIdx].iov_len);
    unsigned int tmpKeyLen = (unsigned int) iov[keyIdx].iov_len;
    unsigned int tmpNextId;

    unsigned int blockid;
    bool needToSplit = false; // 用于当前节点判断是否需分裂后再次插入

    // tmp 用于检验记录是否已存在及获取记录
    // iov 保存了待插入记录所以不能被破坏
    size_t keySize = getKeyBytes(keyType);
    std::vector<char> tmpKey(keySize);
    unsigned int tmpVal;
    std::vector<struct iovec> tmp = {
        {&tmpKey[0], keySize}, {&tmpVal, sizeof(unsigned int)}};

    std::vector<struct iovec> rec; // 上一节点要插入的记录
    std::pair<unsigned int, bool> splitRet;
    std::pair<bool, unsigned int> pret;        

    DataBlock data, next, parent, root; // 复用时无需再 setTable
    data.setTable(table_);
    next.setTable(table_);
    parent.setTable(table_);
    root.setTable(table_);

    while (!stk.empty()) {
        blockid = stk.top();
        data.attachBuffer(&bd, blockid);
        Slot *slots = data.getSlotsPointer();
        unsigned short ret = data.searchRecord(iov[keyIdx].iov_base, iov[keyIdx].iov_len);  

        if (data.getType() == BLOCK_TYPE_DATA) { // 叶节点            
            stk.pop(); // 准备向上回溯   
            getRecord(data.buffer_, slots, ret, tmp);

            // 检查记录是否已存在
            if (memcmp(iov[keyIdx].iov_base, tmp[keyIdx].iov_base, iov[keyIdx].iov_len) == 0) {
                kBuffer.releaseBuf(bd);
                return EFAULT;
            }
            pret = data.insertRecord(iov);
            if (!pret.first && pret.second != (unsigned int) -1) { // Block 空间不足
                splitRet = data.split(pret.second, iov);
                next.attachBuffer(&bd2, splitRet.first);
                next.setType(BLOCK_TYPE_DATA);
                data.setNext(next.getSelf()); // 维护叶节点的单链表

                if (splitRet.second) data.insertRecord(iov);
                else next.insertRecord(iov);

                // 获取新 block 的最小键
                Slot *nextSlots = next.getSlotsPointer();
                tmpRecord.attach(
                    next.buffer_ + be16toh(nextSlots[0].offset),
                    be16toh(nextSlots[0].length));
                tmpRecord.getByIndex(&tmpKeyBuf[0], &tmpKeyLen, keyIdx);
                tmpNextId = next.getSelf();
                int_type->htobe(&tmpNextId);

                rec.clear();
                rec = {
                    {&tmpKeyBuf[0], tmpKeyLen},
                    {&tmpNextId, sizeof(unsigned int)}}; // 都为网络字节序

                kBuffer.releaseBuf(bd2);

                blockid = stk.top();
                parent.attachBuffer(&bd2, blockid);
                pret = parent.insertRecord(rec);
                if (!pret.first && pret.second != (unsigned int) -1) // 父节点需要分裂
                    needToSplit = true;
                
                kBuffer.releaseBuf(bd2);                                
            }
            kBuffer.releaseBuf(bd);

            while (!stk.empty()) { // 开始回溯
                blockid = stk.top();
                stk.pop();

                if (needToSplit) {
                    needToSplit = false;
                    data.attachBuffer(&bd, blockid);
                    splitRet = data.split(pret.second, rec);
                    next.attachBuffer(&bd2, splitRet.first);
                    next.setType(BLOCK_TYPE_INDEX);

                    if (splitRet.second)
                        data.insertRecord(rec);
                    else
                        next.insertRecord(rec);

                    Slot *nextSlots = next.getSlotsPointer();
                    tmpRecord.attach(
                        next.buffer_ + be16toh(nextSlots[0].offset),
                        be16toh(nextSlots[0].length));
                    tmpRecord.getByIndex(&tmpKeyBuf[0], &tmpKeyLen, keyIdx);
                    tmpNextId = next.getSelf();
                    int_type->htobe(&tmpNextId);

                    rec.clear();
                    rec = {
                        {&tmpKeyBuf[0], tmpKeyLen},
                        {&tmpNextId, sizeof(unsigned int)}}; // 都为网络字节序

                    kBuffer.releaseBuf(bd);
                    kBuffer.releaseBuf(bd2);

                    // 尝试给父节点插入中位键
                    blockid = stk.top();
                    parent.attachBuffer(&bd, blockid);
                    pret = parent.insertRecord(rec);
                    if (!pret.first && pret.second != (unsigned int) -1)
                        needToSplit = true;

                    kBuffer.releaseBuf(bd);
                }
            }
            if (needToSplit) { // 根节点需要分裂再插入
                bd = kBuffer.borrow(table_->name_.c_str(), 0); // 获取超块
                super.attach(bd->buffer);

                blockid = super.getRoot();
                data.attachBuffer(&bd2, blockid); // 获取根
                splitRet = data.split(pret.second, rec);
                next.attachBuffer(&bd3, splitRet.first); // 获取新 block
                next.setType(BLOCK_TYPE_INDEX);

                if (splitRet.second)
                    data.insertRecord(rec);
                else
                    next.insertRecord(rec);
                kBuffer.releaseBuf(bd2);
                kBuffer.releaseBuf(bd3);

                Slot *nextSlots = next.getSlotsPointer();
                tmpRecord.attach(
                    next.buffer_ + be16toh(nextSlots[0].offset),
                    be16toh(nextSlots[0].length));
                tmpRecord.getByIndex(&tmpKeyBuf[0], &tmpKeyLen, keyIdx);
                tmpNextId = next.getSelf();
                int_type->htobe(&tmpNextId);

                rec.clear();
                rec = {
                    {&tmpKeyBuf[0], tmpKeyLen},
                    {&tmpNextId, sizeof(unsigned int)}}; // 都为网络字节序

                unsigned int rootId = table_->allocate(); // 申请新 block 作为根
                root.attachBuffer(&bd2, rootId);
                root.insertRecord(rec);
                root.setNext(data.getSelf());
                root.setType(BLOCK_TYPE_INDEX);
                super.setRoot(rootId); // 维护超块中的根 blockid

                kBuffer.releaseBuf(bd);
                kBuffer.releaseBuf(bd2);
            }
            return S_OK;
        } else { // BLOCK_TYPE_INDEX
            // 由于 mergeBlock 中需先删除父节点中的记录再 insert，
            // 所以可能暂时出现无记录、仅有 next 的情况
            if (!data.getSlots()) {
                stk.push(data.getNext()); 

            } else if (ret >= data.getSlots()) {
                getRecord(data.buffer_, slots, data.getSlots() - 1, tmp);
                int_type->betoh(tmp[1].iov_base);

                stk.push(*(unsigned int *) tmp[1].iov_base);               
            } else {
                getRecord(data.buffer_, slots, ret, tmp);

                // 若相等则为键的右侧指针，否则为左侧
                if (memcmp(tmp[keyIdx].iov_base, iov[keyIdx].iov_base, iov[keyIdx].iov_len) == 0) {
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
            kBuffer.releaseBuf(bd);
        }
    }
    return EFAULT;
}

bool DataBlock::borrow(
    int idx,
    unsigned int blockid,
    std::vector<struct iovec> &dataIov)
{
    bool ret = false;
    unsigned short leFreesize = USHRT_MAX, riFreesize = USHRT_MAX;
    unsigned int leftId = -1, rightId = -1, tmpLen = sizeof(unsigned int);
    
    RelationInfo *info = table_->info_;
    unsigned int keyIdx = info->key;
    DataType *keyType = info->fields[keyIdx].type;

    BufDesp *bd = nullptr, *bd2 = nullptr, *bd3 = nullptr;   
    Slot *slots = getSlotsPointer();
    DataType *intType = findDataType("INT");

    DataBlock data, sibling;
    data.setTable(table_);
    sibling.setTable(table_);
    data.attachBuffer(&bd2, blockid);
    
    // 用于记录的插入和删除
    size_t keySize = getKeyBytes(keyType);
    std::vector<char> key(keySize);
    unsigned int val;
    std::vector<struct iovec> iov = {
        {&key[0], keySize}, {&val, sizeof(unsigned int)}};

    // 用于本节点替换原先的中位键
    std::vector<char> splitKey(keySize);
    unsigned int splitVal;
    std::vector<struct iovec> splitIov = {
        {&splitKey[0], keySize}, {&splitVal, sizeof(unsigned int)}};

    // 用于借出最左键时记录原来从左往右第二个键值对
    std::vector<char> tmpKey(keySize);
    unsigned int tmpVal;
    std::vector<struct iovec> tmpIov = {
        {&tmpKey[0], keySize}, {&tmpVal, sizeof(unsigned int)}};

    // 若 data 非最左节点，获取左兄弟 freesize
    if (idx != -1) {        
        DataBlock left;
        left.setTable(table_);
        if (!idx) { // 若 data 为从左往右数第二个子节点
            leftId = getNext();
            left.attachBuffer(&bd, leftId);
        } else {
            // 当对应 slots 中下标不为0时
            Record record;
            record.attach(
                buffer_ + be16toh(slots[idx - 1].offset),
                be16toh(slots[idx - 1].length));
            record.getByIndex((char *) &leftId, &tmpLen, 1);

            intType->betoh(&leftId);
            left.attachBuffer(&bd, leftId);
        }
        leFreesize = left.getFreeSize();
        kBuffer.releaseBuf(bd);
    }

    // 若不为最右节点，获取右兄弟 freesize
    if (idx < getSlots() - 1) {
        Record record;
        record.attach(
            buffer_ + be16toh(slots[idx + 1].offset),
            be16toh(slots[idx + 1].length));       
        record.getByIndex((char *) &rightId, &tmpLen, 1);
        intType->betoh(&rightId); // 保持 leftId 及 rightId 均为主机序

        DataBlock right;
        right.setTable(table_);
        right.attachBuffer(&bd, rightId);
        riFreesize = right.getFreeSize();
        kBuffer.releaseBuf(bd);
    }

    // 因为子节点不为根，所以必然有兄弟节点
    // freesize 越小越可能借出键
    if (leFreesize <= riFreesize) { // 向左兄弟借键
        sibling.attachBuffer(&bd, leftId);

        // 需根据节点类型来确定 iov 结构
        std::vector<struct iovec> &iovRef =
            sibling.getType() == BLOCK_TYPE_DATA ? dataIov : iov;

        getRecord( // 获取最右键
            sibling.buffer_,
            sibling.getSlotsPointer(),
            sibling.getSlots() - 1,
            iovRef);
        sibling.removeRecord(iovRef);
        if (sibling.isUnderflow()) { // 若借出键后会下溢
            sibling.insertRecord(iovRef);
            ret = false;
        } else {
            data.insertRecord(iovRef);

            // 修改两个子节点对应的中位键
            getRecord(buffer_, getSlotsPointer(), idx, splitIov);
            removeRecord(splitIov);

            // 重新获取 data 的第一个键
            getRecord(data.buffer_, data.getSlotsPointer(), 0, iovRef);
    
            // splitIov 的主键字段指向 splitKey
            memcpy(
                &splitKey[0],
                iovRef[data.getType() == BLOCK_TYPE_DATA ? keyIdx : 0].iov_base,
                keySize);      
            splitVal = blockid;
            intType->htobe(&splitVal); // iov 此时的值为被移动的记录

            // 再次插入本节点时仍需考虑分裂
            // 因为尽管 blockid 的长度固定，但主键仍有可能是变长的
            // 由于变长主键比较少见，在此暂不考虑
            insertRecord(splitIov);
            ret = true;
        }
    } else { // 向右兄弟借键
        sibling.attachBuffer(&bd, rightId);
        
        // 兄弟为叶节点时，next 指向下一叶节点而非最左指针
        if (sibling.getType() == BLOCK_TYPE_DATA) {
            getRecord(sibling.buffer_, sibling.getSlotsPointer(), 0, dataIov);
            sibling.removeRecord(dataIov);

            if (sibling.isUnderflow()) {
                sibling.insertRecord(dataIov);
                ret = false;
            } else {
                data.insertRecord(dataIov);

                // 修改两个子节点对应的中位键
                getRecord(buffer_, getSlotsPointer(), (unsigned short) idx + 1, splitIov);
                removeRecord(splitIov);

                // 因为 sibling 的原第一个记录已被删除，
                // 故需重新获取它的第一个记录
                getRecord(
                    sibling.buffer_, sibling.getSlotsPointer(), 0, dataIov);
                memcpy(&splitKey[0], dataIov[keyIdx].iov_base, keySize);
                splitVal = rightId;

                // 注意是转换 splitVal 而非 rightId 的字节序
                intType->htobe(&splitVal); 
                insertRecord(splitIov);                
                ret = true;
            }            
        } else { // BLOCK_TYPE_INDEX
            // 需要到 next 指向的子节点获取最左键
            DataBlock child;
            child.setTable(table_);
            child.attachBuffer(&bd3, sibling.getNext());

            // 获得 child 第一条记录对应的键
            getRecordByIndex(
                child.buffer_, child.getSlotsPointer(), 0, iov[0], 0);
            val = sibling.getNext(); // 此时 iov 即为要借出的键值对
            intType->htobe(&val);    // 保持 iov 都为网络字节序
            kBuffer.releaseBuf(bd3);

            // 修改 sibling 的 next 并重排其记录
            getRecord(sibling.buffer_, sibling.getSlotsPointer(), 0, tmpIov);
            intType->betoh(tmpIov[1].iov_base);
            sibling.setNext(*(unsigned int *) tmpIov[1].iov_base);
            intType->htobe(tmpIov[1].iov_base);
            sibling.removeRecord(tmpIov);

            if (sibling.isUnderflow()) { // 若借出键后会下溢
                intType->betoh(&val);
                sibling.setNext(val); // 将 next 重置为旧值
                intType->htobe(&val);
                sibling.insertRecord(tmpIov);
                ret = false;
            } else {
                data.insertRecord(iov);

                // 修改两个子节点对应的中位键
                getRecord(buffer_, getSlotsPointer(), idx + 1, splitIov);
                removeRecord(splitIov);

                // 重新获取 sibling 的第一个记录
                // 注意内节点使用 tmpIov
                getRecord(
                    sibling.buffer_, sibling.getSlotsPointer(), 0, tmpIov);
                memcpy(&splitKey[0], tmpIov[0].iov_base, keySize);
                splitVal = rightId; // 注意中位键对应的是右侧的 sibling
                intType->htobe(&splitVal);
                insertRecord(splitIov);
                ret = true;
            }
        }      
    }       
    kBuffer.releaseBuf(bd);
    kBuffer.releaseBuf(bd2);
    return ret;
}

void DataBlock::merge(
    int idx,
    unsigned int blockid,
    std::vector<struct iovec> &dataIov)
{
    unsigned short leFreesize = 0, riFreesize = 0;
    unsigned int leftId = -1, rightId = -1, tmpLen = sizeof(unsigned int);   
    BufDesp *bd = nullptr, *bd2;
    Slot *slots = getSlotsPointer();
    DataType *intType = findDataType("INT");

    RelationInfo *info = table_->info_;
    unsigned int keyIdx = info->key;
    DataType *keyType = info->fields[keyIdx].type;

    DataBlock data, sibling;
    data.setTable(table_);
    sibling.setTable(table_);
    data.attachBuffer(&bd2, blockid);

    size_t keySize = getKeyBytes(keyType);
    std::vector<char> tmpKey(keySize);
    unsigned int tmpVal;
    std::vector<struct iovec> tmpIov = {
        {&tmpKey[0], keySize}, {&tmpVal, sizeof(unsigned int)}};

    if (idx != -1) {
        DataBlock left;
        left.setTable(table_);
        if (!idx) { // 从左往右数第二个子节点
            leftId = getNext();
            left.attachBuffer(&bd, leftId);
        } else {
            Record record;
            record.attach(
                buffer_ + be16toh(slots[idx - 1].offset),
                be16toh(slots[idx - 1].length));
            record.getByIndex((char *) &leftId, &tmpLen, 1);

            intType->betoh(&leftId); // 保持 blockid 均为主机序
            left.attachBuffer(&bd, leftId);
        }
        leFreesize = left.getFreeSize();
        kBuffer.releaseBuf(bd);
    }

    if (idx < getSlots() - 1) { // 不为最右的子节点
        Record record;
        record.attach(
            buffer_ + be16toh(slots[idx + 1].offset),
            be16toh(slots[idx + 1].length));
        record.getByIndex((char *) &rightId, &tmpLen, 1);
        intType->betoh(&rightId);

        DataBlock right;
        right.setTable(table_);
        right.attachBuffer(&bd, rightId);
        riFreesize = right.getFreeSize();
        kBuffer.releaseBuf(bd);
    }

    // 选 freesize 更大的兄弟节点合并
    // 对于叶节点，将右合并到左更容易维护单链表
    sibling.attachBuffer(&bd, leFreesize >= riFreesize ? leftId : rightId);
    if (leFreesize >= riFreesize)
        sibling.mergeBlock(blockid, getSelf(), idx, dataIov);
    else
        data.mergeBlock(sibling.getSelf(), getSelf(), idx + 1, dataIov);

    // 设置 data 的 next
    // 若为叶节点，则需维护单链表
    if (data.getType() == BLOCK_TYPE_DATA) {
        if (leFreesize >= riFreesize) {
            sibling.setNext(data.getNext());
            data.setNext(NULL);
        } else {
            data.setNext(sibling.getNext());
            sibling.setNext(NULL);
        }           
    }
    kBuffer.releaseBuf(bd);
    kBuffer.releaseBuf(bd2);
}

void DataBlock::mergeBlock(
    unsigned int blockid,
    unsigned int parentId,
    int blockIdx,
    std::vector<struct iovec> &dataIov)
{
    RelationInfo *info = table_->info_;
    unsigned int keyIdx = info->key;
    DataType *keyType = info->fields[keyIdx].type;

    BufDesp *bd = nullptr, *bd2 = nullptr;
    DataType *intType = findDataType("INT");
    DataBlock data;
    data.setTable(table_);
    data.attachBuffer(&bd, blockid);

    size_t keySize = getKeyBytes(keyType);
    std::vector<char> tmpKey(keySize);
    unsigned int tmpVal;
    std::vector<struct iovec> tmpIov = {
        {&tmpKey[0], keySize}, {&tmpVal, sizeof(unsigned int)}};

    // 必须先删去父节点中 data 对应的记录，
    // 否则 insert 在向下定位时会找到错误的位置
    DataBlock parent;
    parent.setTable(table_);
    parent.attachBuffer(&bd2, parentId);

    if (blockIdx == -1) {
        getRecord(parent.buffer_, parent.getSlotsPointer(), 0, tmpIov);
        parent.removeRecord(tmpIov);
        intType->betoh(tmpIov[1].iov_base);
        parent.setNext(*(unsigned int *) tmpIov[1].iov_base);
    } else {
        getRecord(parent.buffer_, parent.getSlotsPointer(), blockIdx, tmpIov);
        parent.removeRecord(tmpIov);
    }
    kBuffer.releaseBuf(bd2);

    if (data.getType() == BLOCK_TYPE_INDEX) {
        // 本实验假设内节点记录为定长
        // 因此为了效率，使用不需要从根开始搜的 insertRecord
        while (data.getSlots()) {
            getRecord(data.buffer_, data.getSlotsPointer(), 0, tmpIov);
            insertRecord(tmpIov);
            data.removeRecord(tmpIov); // 为了可重用该 block
        }

        // 移动 data 的最左指针
        DataBlock child;
        child.setTable(table_);
        child.attachBuffer(&bd2, data.getNext());

        getRecordByIndex(
            child.buffer_, child.getSlotsPointer(), 0, tmpIov[0], keyIdx);
        tmpVal = data.getNext();
        intType->htobe(&tmpVal);
        insertRecord(tmpIov);

        kBuffer.releaseBuf(bd2);
    } else {        
        // 当合并到叶节点时，因为可能为变长记录，
        // 所以需要使用会处理分裂的 insert 而非 insertRecord
        while (data.getSlots()) {
            getRecord(data.buffer_, data.getSlotsPointer(), 0, dataIov);
            data.removeRecord(dataIov); // 为了可重用该 block                                  
            insert(dataIov);
        }        
    }    
    kBuffer.releaseBuf(bd);
}

void DataBlock::showRecords(unsigned int blockid)
{
    using namespace db;

    RelationInfo *info = table_->info_;
    unsigned int keyIdx = info->key;

    DataType *bigint = findDataType("BIGINT");
    BufDesp *bd = nullptr;
    DataBlock data;
    data.setTable(table_);
    data.attachBuffer(&bd, blockid);
    Slot *slots = data.getSlotsPointer();

    long long tmpKey;
    unsigned int tmpLen = (unsigned int) sizeof(long long);
    std::vector<long long> debugKeys;

    for (int i = 0; i < data.getSlots(); ++i) {
        Record record;
        record.attach(
            data.buffer_ + be16toh(slots[i].offset), be16toh(slots[i].length));
        record.getByIndex((char *) &tmpKey, &tmpLen, 0);

        bigint->betoh(&tmpKey);
        debugKeys.push_back(tmpKey);
    }

    printf("blockid = %u\n", blockid);
    for (int i = 0; i < debugKeys.size(); ++i) {
        printf("%lld ", debugKeys[i]);
    }
    printf("\n\n");

    kBuffer.releaseBuf(bd);
}

int DataBlock::remove(std::vector<struct iovec> &iov)
{
    RelationInfo *info = table_->info_;
    unsigned int keyIdx = info->key;
    DataType *keyType = info->fields[keyIdx].type;
    DataType *intType = findDataType("INT");

    SuperBlock super;
    BufDesp *bd, *bd2 = nullptr;
    bd = kBuffer.borrow(table_->name_.c_str(), 0);
    super.attach(bd->buffer);

    // 存 blockid 及在父节点中的下标
    std::stack<std::pair<unsigned int, int>> stk;
    std::pair<unsigned int, int> blockInfo;
    stk.push({super.getRoot(), -1});
    kBuffer.releaseBuf(bd); // 释放超块
   
    // preRet 用于存放本节点在父节点 slots 中的下标
    // 本节点对应了最左边的指针时，preRet == -1 表明父节点应使用 next 来索引
    int ret, preRet;
    unsigned int parentId;

    // 用于在向下定位时暂存内节点搜到的记录
    size_t keySize = getKeyBytes(keyType);
    std::vector<char> tmpKey(keySize);
    unsigned int tmpVal;
    std::vector<struct iovec> tmp = {
        {&tmpKey[0], keySize}, {&tmpVal, sizeof(unsigned int)}};

    DataBlock data, parent;
    data.setTable(table_);
    parent.setTable(table_);
    
    while (!stk.empty()) {
        blockInfo = stk.top();
        preRet = blockInfo.second;

        data.attachBuffer(&bd, blockInfo.first);
        Slot *slots = data.getSlotsPointer();
        ret = (int) data.searchRecord(iov[keyIdx].iov_base, iov[keyIdx].iov_len);

        if (data.getType() == BLOCK_TYPE_DATA) { // 叶节点
            stk.pop();                           // 准备向上回溯
            if (!data.removeRecord(iov)) {       // 记录不存在
                kBuffer.releaseBuf(bd);
                return EFAULT;
            }

            // 若为根节点则无需处理下溢
            bd2 = kBuffer.borrow(table_->name_.c_str(), 0);
            super.attach(bd2->buffer);
            if (blockInfo.first == super.getRoot()) { 
                kBuffer.releaseBuf(bd2);
                kBuffer.releaseBuf(bd);
                return S_OK;
            }
            kBuffer.releaseBuf(bd2);

            // 删除后下溢
            if (data.isUnderflow()) { 
                parentId = stk.top().first;
                parent.attachBuffer(&bd2, parentId);
                if (!parent.borrow(preRet, data.getSelf(), iov)) { // 借键失败
                    parent.merge(preRet, data.getSelf(), iov);
                }
                kBuffer.releaseBuf(bd2);                
            }
            kBuffer.releaseBuf(bd);

            while (!stk.empty()) { // 向上回溯
                blockInfo = stk.top();
                stk.pop();
                preRet = blockInfo.second;

                data.attachBuffer(&bd, blockInfo.first);
                if (data.isUnderflow()) {
                    if (!stk.empty()) { // 不是根节点
                        parentId = stk.top().first;
                        parent.attachBuffer(&bd2, parentId);
                        if (!parent.borrow(
                                preRet, data.getSelf(), iov)) { // 借键失败
                            // 无需调用 merge 后检查 parent 是否下溢，
                            // 因为下一轮会对其检查
                            parent.merge(preRet, data.getSelf(), iov);
                        }
                        kBuffer.releaseBuf(bd2); 
                    } else {
                        // 根节点下溢分两种情况：
                        // 只剩一个指针时，将根节点删除，并将根设为原来的唯一子节点
                        // 否则，根节点需保留
                        if (!data.getSlots()) {
                            bd2 = kBuffer.borrow(table_->name_.c_str(), 0);
                            super.attach(bd2->buffer);
                            super.setRoot(data.getNext());
                            data.setNext(NULL);
                            kBuffer.releaseBuf(bd2);
                        }
                        kBuffer.releaseBuf(bd);
                        return S_OK;
                    }
                }               
                kBuffer.releaseBuf(bd);
            }
            return S_OK;
        } else { // BLOCK_TYPE_INDEX
            if (ret >= (int) data.getSlots()) {
                getRecord(data.buffer_, slots, data.getSlots() - 1, tmp);
                intType->betoh(tmp[1].iov_base);
                stk.push(
                    {*(unsigned int *) tmp[1].iov_base, data.getSlots() - 1});
            } else {
                getRecord(data.buffer_, slots, (unsigned short) ret, tmp);
                
                // 若相等则为键的右侧指针，否则为左侧
                if (memcmp(
                        tmp[keyIdx].iov_base,
                        iov[keyIdx].iov_base,
                        iov[keyIdx].iov_len) == 0) {
                    intType->betoh(tmp[1].iov_base);

                    stk.push({*(unsigned int *) tmp[1].iov_base, ret});
                } else if (ret > 0) {
                    getRecord(data.buffer_, slots, (unsigned short) ret - 1, tmp);
                    intType->betoh(tmp[1].iov_base);

                    stk.push({*(unsigned int *) tmp[1].iov_base, ret - 1});
                } else {
                    stk.push({data.getNext(), -1}); // 最左指针对应下标为 -1
                }
            }
            kBuffer.releaseBuf(bd);
        }     
    }
    return EFAULT;
}

int DataBlock::update(std::vector<struct iovec> &iov)
{
    if (remove(iov) == S_OK && insert(iov) == S_OK)
        return S_OK;
    else
        return EFAULT;
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
