////
// @file blockTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/block.h>
#include <db/record.h>
#include <db/buffer.h>
#include <db/file.h>
#include <db/table.h>

#define SHORT_ADDR "The Old Schools, Trinity Ln, Cambridge CB2 1TN, UK"
#define LONG_ADDR "1234 Elm Street, Apartment 567, Willow Creek Meadows, Suite 890, Northwood Heights, Building 1011, Block A, Pineview Avenue, Tower 12, Oakwood Plaza, Unit 3456, Maple Ridge, Floor 7, Birchwood Lane, Lot 8910, Cedar Valley, Villa 12345, Redwood Grove, Estate 6789, Magnolia Court, Manor 2468, Sunflower Circle, Crescent 13579, Rosewood Lane, Garden 369, Lily Pond, Terrace 2468, Juniper Way, Cove 1011, Aspen Ridge, Chalet 7890, Birch Hill, Lodge 5678, Cedar Lane, Cabin 1234, Pinecrest, Retreat 5678, Willowbrook, Haven 9101, Oakdale, Sanctuary 2345, Maplewood, Oasis 6789, Birchwood, Paradise 1011, Cedarwood, Hideaway 1213, Pineview, Serenity 1415, Redwood, Tranquility 1617, Magnolia, Peaceful Place 1819, Sunflower, Blissful Haven 2021, Rosewood, Harmony House 2223, Lily, Calm Corner 2425, Juniper, Quiet Retreat 2627, Aspen, Zen Garden 2829, Birch, Solitude 3031, Cedar, Relaxation Retreat 3233, Pine, Serene Spot 3435."

using namespace db;

TEST_CASE("db/block.h")
{
    SECTION("size")
    {
        REQUIRE(sizeof(CommonHeader) == sizeof(int) * 3);
        REQUIRE(sizeof(Trailer) == 2 * sizeof(int));
        REQUIRE(sizeof(Trailer) % 8 == 0);
        REQUIRE(
            sizeof(SuperHeader) ==
            sizeof(CommonHeader) + sizeof(TimeStamp) + 9 * sizeof(int));
        REQUIRE(sizeof(SuperHeader) % 8 == 0);
        REQUIRE(sizeof(IdleHeader) == sizeof(CommonHeader) + sizeof(int));
        REQUIRE(sizeof(IdleHeader) % 8 == 0);
        REQUIRE(
            sizeof(DataHeader) == sizeof(CommonHeader) + 2 * sizeof(int) +
                                      sizeof(TimeStamp) + 2 * sizeof(short));
        REQUIRE(sizeof(DataHeader) % 8 == 0);
    }

    SECTION("super")
    {
        SuperBlock super;
        unsigned char buffer[SUPER_SIZE];
        super.attach(buffer);
        super.clear(3);

        // magic number：0x64623031
        REQUIRE(buffer[0] == 0x64);
        REQUIRE(buffer[1] == 0x62);
        REQUIRE(buffer[2] == 0x30);
        REQUIRE(buffer[3] == 0x31);

        unsigned short type = super.getType();
        REQUIRE(type == BLOCK_TYPE_SUPER);
        unsigned short freespace = super.getFreeSpace();
        REQUIRE(freespace == sizeof(SuperHeader));

        unsigned int spaceid = super.getSpaceid();
        REQUIRE(spaceid == 3);

        unsigned int idle = super.getIdle();
        REQUIRE(idle == 0);

        TimeStamp ts = super.getTimeStamp();
        char tb[64];
        REQUIRE(ts.toString(tb, 64));
        // printf("ts=%s\n", tb);
        TimeStamp ts1;
        ts1.now();
        REQUIRE(ts < ts1);

        REQUIRE(super.checksum());
    }

    SECTION("data")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1, 3, BLOCK_TYPE_DATA);

        // magic number：0x64623031
        REQUIRE(buffer[0] == 0x64);
        REQUIRE(buffer[1] == 0x62);
        REQUIRE(buffer[2] == 0x30);
        REQUIRE(buffer[3] == 0x31);

        unsigned int spaceid = data.getSpaceid();
        REQUIRE(spaceid == 1);

        unsigned short type = data.getType();
        REQUIRE(type == BLOCK_TYPE_DATA);

        unsigned short freespace = data.getFreeSpace();
        REQUIRE(freespace == sizeof(DataHeader));

        unsigned int next = data.getNext();
        REQUIRE(next == 0);

        unsigned int self = data.getSelf();
        REQUIRE(self == 3);

        TimeStamp ts = data.getTimeStamp();
        char tb[64];
        REQUIRE(ts.toString(tb, 64));
        // printf("ts=%s\n", tb);
        TimeStamp ts1;
        ts1.now();
        REQUIRE(ts < ts1);

        unsigned short slots = data.getSlots();
        REQUIRE(slots == 0);

        REQUIRE(data.getFreeSize() == data.getFreespaceSize());

        REQUIRE(data.checksum());

        REQUIRE(data.getTrailerSize() == 8);
        Slot *pslots =
            reinterpret_cast<Slot *>(buffer + BLOCK_SIZE - sizeof(Slot));
        REQUIRE(pslots == data.getSlotsPointer());
        REQUIRE(data.getFreespaceSize() == BLOCK_SIZE - 8 - sizeof(DataHeader));

        // 假设有5个slots槽位
        data.setSlots(5);
        REQUIRE(data.getTrailerSize() == sizeof(Slot) * 5 + sizeof(int));
        pslots =
            reinterpret_cast<Slot *>(buffer + BLOCK_SIZE - sizeof(Slot)) - 5;
        REQUIRE(pslots == data.getSlotsPointer());
        REQUIRE(
            data.getFreespaceSize() ==
            BLOCK_SIZE - data.getTrailerSize() - sizeof(DataHeader));
    }

    SECTION("allocate")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1, 3, BLOCK_TYPE_DATA);

        // 分配8字节
        std::pair<unsigned char *, bool> alloc_ret = data.allocate(8, 0);
        REQUIRE(alloc_ret.first == buffer + sizeof(DataHeader));
        REQUIRE(data.getFreeSpace() == sizeof(DataHeader) + 8);
        REQUIRE(
            data.getFreeSize() ==
            BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer) - 8);
        REQUIRE(data.getSlots() == 1);
        Slot *pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[0].length) == 8);
        REQUIRE(data.getTrailerSize() == 8);

        // 随便写一个记录
        Record record;
        record.attach(buffer + sizeof(DataHeader), 8);
        std::vector<struct iovec> iov(1);
        int kkk = 3;
        iov[0].iov_base = (void *) &kkk;
        iov[0].iov_len = sizeof(int);
        unsigned char h = 0;
        record.set(iov, &h);

        // 分配5字节
        alloc_ret = data.allocate(5, 0);
        REQUIRE(alloc_ret.first == buffer + sizeof(DataHeader) + 8);
        REQUIRE(data.getFreeSpace() == sizeof(DataHeader) + 2 * 8);
        REQUIRE(
            data.getFreeSize() ==
            BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer) - 3 * 8);
        REQUIRE(data.getSlots() == 2);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader) + 8);
        REQUIRE(be16toh(pslots[0].length) == 8);
        REQUIRE(be16toh(pslots[1].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[1].length) == 8);
        REQUIRE(data.getTrailerSize() == 16);

        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        kkk = 4;
        iov[0].iov_base = (void *) &kkk;
        iov[0].iov_len = sizeof(int);
        record.set(iov, &h);

        // 分配711字节
        alloc_ret = data.allocate(711, 0);
        REQUIRE(alloc_ret.first == buffer + sizeof(DataHeader) + 8 * 2);
        REQUIRE(data.getFreeSpace() == sizeof(DataHeader) + 2 * 8 + 712);
        REQUIRE(
            data.getFreeSize() ==
            BLOCK_SIZE - sizeof(DataHeader) - sizeof(Trailer) - 3 * 8 - 712);
        REQUIRE(data.getSlots() == 3);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - 3 * sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader) + 16);
        REQUIRE(be16toh(pslots[0].length) == 712);
        REQUIRE(data.getTrailerSize() == 16);

        record.attach(buffer + sizeof(DataHeader) + 2 * 8, 712);
        char ggg[711 - 4];
        iov[0].iov_base = (void *) ggg;
        iov[0].iov_len = 711 - 4;
        record.set(iov, &h);
        REQUIRE(record.length() == 711);

        // 回收第2个空间
        unsigned short size = data.getFreeSize();
        data.deallocate(1);
        REQUIRE(data.getFreeSize() == size + 8);
        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        REQUIRE(!record.isactive());

        REQUIRE(data.getSlots() == 2);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader) + 16);
        REQUIRE(be16toh(pslots[0].length) == 712);
        REQUIRE(be16toh(pslots[1].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[1].length) == 8);
        REQUIRE(data.getTrailerSize() == 16);

        data.shrink();
        size = data.getFreeSize();
        REQUIRE(
            size ==
            BLOCK_SIZE - sizeof(DataHeader) - data.getTrailerSize() - 8 - 712);
        unsigned short freespace = data.getFreeSpace();
        REQUIRE(freespace == sizeof(DataHeader) + 8 + 712);

        REQUIRE(data.getSlots() == 2);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[0].length) == 8);
        REQUIRE(be16toh(pslots[1].offset) == sizeof(DataHeader) + 8);
        REQUIRE(be16toh(pslots[1].length) == 712);
        REQUIRE(data.getTrailerSize() == 16);

        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        REQUIRE(record.isactive());

        // 回收第3个空间
        size = data.getFreeSize();
        data.deallocate(1);
        REQUIRE(data.getFreeSize() == size + 712 + 8);
        record.attach(buffer + sizeof(DataHeader) + 8, 8);
        REQUIRE(!record.isactive());

        REQUIRE(data.getSlots() == 1);
        pslots = data.getSlotsPointer();
        REQUIRE(
            (unsigned char *) pslots ==
            buffer + BLOCK_SIZE - sizeof(int) - sizeof(Slot));
        REQUIRE(be16toh(pslots[0].offset) == sizeof(DataHeader));
        REQUIRE(be16toh(pslots[0].length) == 8);
        REQUIRE(data.getTrailerSize() == 8);

        // 回收第1个空间
        size = data.getFreeSize();
        data.deallocate(0);
        REQUIRE(data.getFreeSize() == size + 8);
        record.attach(buffer + sizeof(DataHeader), 8);
        REQUIRE(!record.isactive());

        // shrink
        data.shrink();
        size = data.getFreeSize();
        REQUIRE(
            size == BLOCK_SIZE - sizeof(DataHeader) - data.getTrailerSize());
        freespace = data.getFreeSpace();
        REQUIRE(freespace == sizeof(DataHeader));
    }

    SECTION("sort")
    {
        char x[3] = {'k', 'a', 'e'};
        std::sort(x, x + 3);
        REQUIRE(x[0] == 'a');
        REQUIRE(x[1] == 'e');
        REQUIRE(x[2] == 'k');
    }

    SECTION("reorder")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1, 3, BLOCK_TYPE_DATA);

        // 假设表的字段是：id, char[12], varchar[512]
        std::vector<struct iovec> iov(3);
        DataType *type = findDataType("BIGINT");

        // 第1条记录
        long long id = 12;
        type->htobe(&id);
        iov[0].iov_base = &id;
        iov[0].iov_len = 8;
        iov[1].iov_base = "John Carter ";
        iov[1].iov_len = 12;
        const char *addr = "(323) 238-0693"
                           "909 - 1/2 E 49th St"
                           "Los Angeles, California(CA), 90011";
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = strlen(addr);

        // 分配空间
        unsigned short len = (unsigned short) Record::size(iov);
        std::pair<unsigned char *, bool> alloc_ret = data.allocate(len, 0);
        // 填充记录
        Record record;
        record.attach(alloc_ret.first, len);
        unsigned char header = 0;
        record.set(iov, &header);
        // 重新排序
        data.reorder(type, 0);

        REQUIRE(data.getFreeSpace() == sizeof(DataHeader) + len + 3);
        Slot *slot =
            (Slot *) (buffer + BLOCK_SIZE - sizeof(int) - sizeof(Slot));
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len + 3);
        REQUIRE(data.getSlots() == 1);

        // 第2条记录
        id = 3;
        type->htobe(&id);
        iov[0].iov_base = &id;
        iov[0].iov_len = 8;
        iov[1].iov_base = "Joi Biden    ";
        iov[1].iov_len = 12;
        const char *addr2 = "(323) 751-1875"
                            "7609 Mckinley Ave"
                            "Los Angeles, California(CA), 90001";
        iov[2].iov_base = (void *) addr2;
        iov[2].iov_len = strlen(addr2);

        // 分配空间
        unsigned short len2 = len;
        len = (unsigned short) Record::size(iov);
        alloc_ret = data.allocate(len, 0);
        // 填充记录
        record.attach(alloc_ret.first, len);
        record.set(iov, &header);
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len + 5);
        --slot;
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader) + len2 + 3);
        REQUIRE(be16toh(slot->length) == len + 5);
        // 重新排序
        data.reorder(type, 0);

        slot = (Slot *) (buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader) + len2 + 3);
        REQUIRE(be16toh(slot->length) == len + 5);
        ++slot;
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len2 + 3);

        // 按照name排序
        type = findDataType("CHAR");
        data.reorder(type, 1);
        slot = (Slot *) (buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len2 + 3);
        ++slot;
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader) + len2 + 3);
        REQUIRE(be16toh(slot->length) == len + 5);
    }

    SECTION("lowerbound")
    {
        char x[4] = {'a', 'c', 'e', 'k'};
        char s = 'e';
        char *ret = std::lower_bound(x, x + 4, s);
        REQUIRE(ret == x + 2);

        // b总是搜索值
        struct Comp
        {
            char val;
            bool operator()(char a, char b)
            {
                REQUIRE(b == -1);
                return a < val;
            }
        };
        Comp comp;
        comp.val = 'd';
        s = -1;
        ret = std::lower_bound(x, x + 4, s, comp);
        REQUIRE(ret == x + 2);
    }

    SECTION("search")
    {
        DataBlock data;
        unsigned char buffer[BLOCK_SIZE];

        data.attach(buffer);
        data.clear(1, 3, BLOCK_TYPE_DATA);

        // 假设表的字段是：id, char[12], varchar[512]
        std::vector<struct iovec> iov(3);
        DataType *type = findDataType("BIGINT");

        // 第1条记录
        long long id = 12;
        type->htobe(&id);
        iov[0].iov_base = &id;
        iov[0].iov_len = 8;
        iov[1].iov_base = "John Carter ";
        iov[1].iov_len = 12;
        const char *addr = "(323) 238-0693"
                           "909 - 1/2 E 49th St"
                           "Los Angeles, California(CA), 90011";
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = strlen(addr);

        // 分配空间
        unsigned short len = (unsigned short) Record::size(iov);
        std::pair<unsigned char *, bool> alloc_ret = data.allocate(len, 0);
        // 填充记录
        Record record;
        record.attach(alloc_ret.first, len);
        unsigned char header = 0;
        record.set(iov, &header);
        // 重新排序
        data.reorder(type, 0);
        // 重设校验和
        data.setChecksum();

        // 第2条记录
        id = 3;
        type->htobe(&id);
        iov[0].iov_base = &id;
        iov[0].iov_len = 8;
        iov[1].iov_base = "Joi Biden    ";
        iov[1].iov_len = 12;
        const char *addr2 = "(323) 751-1875"
                            "7609 Mckinley Ave"
                            "Los Angeles, California(CA), 90001";
        iov[2].iov_base = (void *) addr2;
        iov[2].iov_len = strlen(addr2);

        // 分配空间
        unsigned short len2 = len;
        len = (unsigned short) Record::size(iov);
        alloc_ret = data.allocate(len, 0);
        // 填充记录
        record.attach(alloc_ret.first, len);
        record.set(iov, &header);
        // 重新排序
        data.reorder(type, 0);

        Slot *slot =
            (Slot *) (buffer + BLOCK_SIZE - sizeof(int) - 2 * sizeof(Slot));
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader) + len2 + 3);
        REQUIRE(be16toh(slot->length) == len + 5);
        ++slot;
        REQUIRE(be16toh(slot->offset) == sizeof(DataHeader));
        REQUIRE(be16toh(slot->length) == len2 + 3);

        // 搜索
        id = htobe64(3);
        unsigned short ret = type->search(buffer, 0, &id, sizeof(id));
        REQUIRE(ret == 0);
        id = htobe64(12);
        ret = type->search(buffer, 0, &id, sizeof(id));
        REQUIRE(ret == 1);
        id = htobe64(2);
        ret = type->search(buffer, 0, &id, sizeof(id));
        REQUIRE(ret == 0);
    }

    SECTION("insert")
    {
        Table table;
        table.open("table");

        // 从buffer中出借table:0
        BufDesp *bd = kBuffer.borrow("table", 0);
        REQUIRE(bd);
        // 将bd上buffer挂到super上
        SuperBlock super;
        super.attach(bd->buffer);
        int id = super.getFirst();
        REQUIRE(id == 1);
        int idle = super.getIdle();
        REQUIRE(idle == 0);
        // 释放buffer
        kBuffer.releaseBuf(bd);

        // 加载第1个data
        DataBlock data;
        // 设定block的meta
        data.setTable(&table);
        // 关联数据
        bd = kBuffer.borrow("table", 1);
        data.attach(bd->buffer);

        // 检查block，table表是空的，未添加任何表项
        REQUIRE(data.checksum());
        unsigned short size = data.getFreespaceSize();
        REQUIRE(
            BLOCK_SIZE - sizeof(DataHeader) - data.getTrailerSize() == size);

        // table = id(BIGINT)+phone(CHAR[20])+name(VARCHAR)
        // 准备添加
        DataType *type = findDataType("BIGINT");
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        // 第1条记录
        nid = 7;
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;
        unsigned short osize = data.getFreespaceSize();
        unsigned short nsize = data.requireLength(iov);
        REQUIRE(nsize == 168);
        std::pair<bool, unsigned short> ret = data.insertRecord(iov);
        REQUIRE(ret.first);
        REQUIRE(ret.second == 0);
        REQUIRE(data.getFreespaceSize() == osize - nsize);
        REQUIRE(data.getSlots() == 1);
        Slot *slots = data.getSlotsPointer();
        Record record;
        record.attach(
            data.buffer_ + be16toh(slots[0].offset), be16toh(slots[0].length));
        REQUIRE(record.length() == Record::size(iov));
        REQUIRE(record.fields() == 3);
        long long xid;
        unsigned int len;
        record.getByIndex((char *) &xid, &len, 0);
        REQUIRE(len == 8);
        type->betoh(&xid);
        REQUIRE(xid == 7);
        unsigned char *pid;
        xid = 0;
        record.refByIndex(&pid, &len, 0);
        REQUIRE(len == 8);
        memcpy(&xid, pid, len);
        type->betoh(&xid);
        REQUIRE(xid == 7);

        // 第2条记录
        nid = 3;
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;
        osize = data.getFreespaceSize();
        nsize = data.requireLength(iov);
        REQUIRE(nsize == 176);
        ret = data.insertRecord(iov);
        REQUIRE(ret.first);
        REQUIRE(ret.second == 0);
        REQUIRE(data.getFreespaceSize() == osize - nsize);
        REQUIRE(data.getSlots() == 2);
        slots = data.getSlotsPointer();
        record.attach(
            data.buffer_ + be16toh(slots[0].offset), be16toh(slots[0].length));
        REQUIRE(record.length() == Record::size(iov));
        REQUIRE(record.fields() == 3);
        record.getByIndex((char *) &xid, &len, 0);
        REQUIRE(len == 8);
        type->betoh(&xid);
        REQUIRE(xid == 3);
        xid = 0;
        record.refByIndex(&pid, &len, 0);
        REQUIRE(len == 8);
        memcpy(&xid, pid, len);
        type->betoh(&xid);
        REQUIRE(xid == 3);

        // 第3条
        nid = 11;
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;
        osize = data.getFreespaceSize();
        nsize = data.requireLength(iov);
        REQUIRE(nsize == 168);
        ret = data.insertRecord(iov);
        REQUIRE(ret.first);
        REQUIRE(ret.second == 2);
        REQUIRE(data.getFreespaceSize() == osize - nsize);
        REQUIRE(data.getSlots() == 3);
        slots = data.getSlotsPointer();
        record.attach(
            data.buffer_ + be16toh(slots[2].offset), be16toh(slots[2].length));
        REQUIRE(record.length() == Record::size(iov));
        REQUIRE(record.fields() == 3);
        record.getByIndex((char *) &xid, &len, 0);
        REQUIRE(len == 8);
        type->betoh(&xid);
        REQUIRE(xid == 11);
        xid = 0;
        record.refByIndex(&pid, &len, 0);
        REQUIRE(len == 8);
        memcpy(&xid, pid, len);
        type->betoh(&xid);
        REQUIRE(xid == 11);

        // 第4条 3 7 11
        nid = 5;
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;
        osize = data.getFreespaceSize();
        nsize = data.requireLength(iov);
        REQUIRE(nsize == 176);
        ret = data.insertRecord(iov);
        REQUIRE(ret.first);
        REQUIRE(ret.second == 1);
        REQUIRE(data.getFreespaceSize() == osize - nsize);
        REQUIRE(data.getSlots() == 4);
        slots = data.getSlotsPointer();
        record.attach(
            data.buffer_ + be16toh(slots[1].offset), be16toh(slots[1].length));
        REQUIRE(record.length() == Record::size(iov));
        REQUIRE(record.fields() == 3);
        record.getByIndex((char *) &xid, &len, 0);
        REQUIRE(len == 8);
        type->betoh(&xid);
        REQUIRE(xid == 5);
        xid = 0;
        record.refByIndex(&pid, &len, 0);
        REQUIRE(len == 8);
        memcpy(&xid, pid, len);
        type->betoh(&xid);
        REQUIRE(xid == 5);

        // 键重复，无法插入
        ret = data.insertRecord(iov);
        REQUIRE(!ret.first);
        REQUIRE(ret.second == (unsigned short) -1);

        // 写入，释放
        kBuffer.writeBuf(bd);
        kBuffer.releaseBuf(bd);
    }

    SECTION("iterator")
    {
        Table table;
        table.open("table");

        // 加载第1个data
        DataBlock data;
        // 设定block的meta
        data.setTable(&table);
        // 关联数据
        BufDesp* bd = kBuffer.borrow("table", 1);
        data.attach(bd->buffer);

        DataBlock::RecordIterator ri = data.beginrecord();
        REQUIRE(ri.index == 0);
        unsigned char* pkey;
        unsigned int len;
        ri->refByIndex(&pkey, &len, 0);
        long long key;
        memcpy(&key, pkey, len);
        key = be64toh(key);
        REQUIRE(key == 3); // 3 5 7 11

        ++ri;
        ri->refByIndex(&pkey, &len, 0);
        memcpy(&key, pkey, len);
        key = be64toh(key);
        REQUIRE(key == 5); // 3 5 7 11

        ri++;
        ri->refByIndex(&pkey, &len, 0);
        memcpy(&key, pkey, len);
        key = be64toh(key);
        REQUIRE(key == 7); // 3 5 7 11

        --ri;
        ri->refByIndex(&pkey, &len, 0);
        memcpy(&key, pkey, len);
        key = be64toh(key);
        REQUIRE(key == 5); // 3 5 7 11

        ri--;
        ri->refByIndex(&pkey, &len, 0);
        memcpy(&key, pkey, len);
        key = be64toh(key);
        REQUIRE(key == 3); // 3 5 7 11

        --ri;
        bool ret = ri == data.endrecord();
        REQUIRE(ret);

        ri += 2;
        ri->refByIndex(&pkey, &len, 0);
        memcpy(&key, pkey, len);
        key = be64toh(key);
        REQUIRE(key == 5); // 3 5 7 11

        ri -= 3;
        ri->refByIndex(&pkey, &len, 0);
        memcpy(&key, pkey, len);
        key = be64toh(key);
        REQUIRE(key == 11); // 3 5 7 11

        kBuffer.releaseBuf(bd);
    }
}

inline void htobeIov(
    DataType *bigint, DataType *char_type, DataType *varchar,
    long long* nid, char* phone, char* addr)
{
    if (bigint) bigint->htobe(nid);
    if (char_type) char_type->htobe(phone);
    if (varchar) varchar->htobe(addr);
}

inline void setIov(
    std::vector<struct iovec>& iov,
    long long *nid,
    char *phone,
    void *addr)
{
    iov[0].iov_base = nid;
    iov[0].iov_len = 8;
    iov[1].iov_base = phone;
    iov[1].iov_len = 20;
    iov[2].iov_base = addr;
    iov[2].iov_len = strlen((char *) addr);
}

TEST_CASE("BlockTest", "[p1]")
{
    SECTION("update")
    {
        Table table;
        REQUIRE(table.open("table") == S_OK);

        BufDesp *bd = kBuffer.borrow("table", 0);
        REQUIRE(bd);
        SuperBlock super;
        super.attach(bd->buffer);
        kBuffer.releaseBuf(bd);

        // 加载第 1 个 data
        DataBlock data;
        // 设定 block 的 meta
        data.setTable(&table);
        // 关联数据
        bd = kBuffer.borrow("table", 1);
        data.attach(bd->buffer);

        // 检查 block 是空的
        unsigned short size = data.getFreespaceSize();
        REQUIRE(
            BLOCK_SIZE - sizeof(DataHeader) - data.getTrailerSize() == size);

        // 准备添加
        DataType *bigint = findDataType("BIGINT");
        DataType *char_type = findDataType("CHAR");
        DataType *varchar = findDataType("VARCHAR");

        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char *addr = SHORT_ADDR;
        
        // 第1条记录
        nid = 1;
        strcpy_s(phone, "11111111111");
        htobeIov(bigint, char_type, varchar, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);        
        unsigned short osize = data.getFreespaceSize();
        unsigned short nsize = data.requireLength(iov);

        std::pair<bool, unsigned short> ret = data.insertRecord(iov);
        REQUIRE(ret.first);
        REQUIRE(ret.second == 0);
        REQUIRE(data.getFreespaceSize() == osize - nsize);  // 检查 free space
        REQUIRE(data.getSlots() == 1);  // 检查此时只有1条记录
        Slot *slots = data.getSlotsPointer();
        Record record;
        record.attach(
            data.buffer_ + be16toh(slots[0].offset), be16toh(slots[0].length));
        REQUIRE(record.length() == Record::size(iov));
        REQUIRE(record.fields() == 3);

        // 测试 getByIndex
        long long xid;
        unsigned int len;
        record.getByIndex((char *) &xid, &len, 0);
        REQUIRE(len == 8);
        bigint->betoh(&xid);
        REQUIRE(xid == 1);

        char str[22];
        len = sizeof(str);
        REQUIRE(record.getByIndex(str, &len, 1));
        REQUIRE(len == 20);
        char_type->betoh(str);
        REQUIRE(strcmp(str, phone) == 0);
        
        // 测试 refByIndex
        unsigned char *pid;        
        xid = -1;
        len = sizeof(&pid);
        record.refByIndex(&pid, &len, 0);
        REQUIRE(len == 8);
        memcpy(&xid, pid, len);
        bigint->betoh(&xid);
        REQUIRE(xid == 1);

        // 更新第1条记录
        nid = 1;
        strcpy_s(phone, "222222222222");
        htobeIov(bigint, char_type, varchar, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        unsigned short freesize = data.getFreeSize();

        REQUIRE(data.updateRecord(iov));
        REQUIRE(data.getFreeSize() == freesize);
        REQUIRE(data.getSlots() == 1);
        slots = data.getSlotsPointer();
        record.attach(
            data.buffer_ + be16toh(slots[0].offset), be16toh(slots[0].length));
        REQUIRE(record.length() == Record::size(iov));
        REQUIRE(record.fields() == 3);

        // 测试 getByIndex
        len = sizeof(&xid);
        record.getByIndex((char *) &xid, &len, 0);
        REQUIRE(len == 8);
        bigint->betoh(&xid);
        REQUIRE(xid == 1);

        len = sizeof(str);
        record.getByIndex(str, &len, 1);
        REQUIRE(len == 20);
        char_type->betoh(str);
        REQUIRE(strcmp(str, phone) == 0);

        // 插入直到即将分裂     
        while (ret.first) {
            bigint->betoh(&nid);
            nid += 1;
            bigint->htobe(&nid);
            setIov(iov, &nid, phone, (void *) addr);            
            ret = data.insertRecord(iov);            
        }

        bigint->betoh(&nid);
        REQUIRE(nid == 178); // 共能插入 177 条记录

        // 测试记录不存在时 update
        htobeIov(bigint, char_type, varchar, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(!data.updateRecord(iov));
       
        // 使更新后的记录更长
        addr = LONG_ADDR;
        bigint->betoh(&nid);
        nid -= 1;
        htobeIov(bigint, char_type, varchar, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(!data.getNext());  // 检查此时没有新 block
        REQUIRE(data.updateRecord(iov));
       
        kBuffer.writeBuf(bd);
        kBuffer.releaseBuf(bd);

        // 检查分裂出新 block
        unsigned int blockid = data.getNext();
        bd = kBuffer.borrow("table", blockid);
        REQUIRE(bd);

        // 检查新记录插在了新 block 中       
        DataBlock next;
        next.setTable(&table);
        next.attach(bd->buffer);
        slots = next.getSlotsPointer();

        // 因为主键是递增的，所以新记录一定是新 block 的最后一条
        record.attach(
            next.buffer_ + be16toh(slots[next.getSlots() - 1].offset),
            be16toh(slots[next.getSlots() - 1].length));
        len = sizeof(LONG_ADDR);
        std::vector<char> tmp_addr(len);
        record.getByIndex(&tmp_addr[0], &len, 2);
        char_type->betoh(&tmp_addr[0]);
        REQUIRE(strcmp(&tmp_addr[0], addr) == 0);

        kBuffer.releaseBuf(bd);
    }

    SECTION("remove")
    {
        Table table;
        REQUIRE(table.open("table") == S_OK);

        DataBlock data;
        data.setTable(&table);
        BufDesp *bd = kBuffer.borrow("table", 1);
        REQUIRE(bd);
        data.attach(bd->buffer);
        
        DataType *bigint = findDataType("BIGINT");
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char *addr = SHORT_ADDR;

        // 测试记录不存在
        nid = 1;
        htobeIov(bigint, nullptr, nullptr, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        data.clear(1, 1, BLOCK_TYPE_DATA);
        REQUIRE(!data.removeRecord(iov));

        // 测试记录存在
        REQUIRE(data.insertRecord(iov).first);
        REQUIRE(data.removeRecord(iov));

        // 先插入多条记录
        nid = 2;
        htobeIov(bigint, nullptr, nullptr, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(data.insertRecord(iov).first);

        nid = 4;
        htobeIov(bigint, nullptr, nullptr, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(data.insertRecord(iov).first);

        nid = 5;
        htobeIov(bigint, nullptr, nullptr, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(data.insertRecord(iov).first);

        // 测试待删除主键的值虽在范围中但不存在
        nid = 3;
        htobeIov(bigint, nullptr, nullptr, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(!data.removeRecord(iov));

        // 测试记录存在
        nid = 4;
        htobeIov(bigint, nullptr, nullptr, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(data.removeRecord(iov));

        kBuffer.releaseBuf(bd);
    }
}

// 在函数内先调用 htobe
// keybuf 和 idbuf 在调用后为网络字节序
inline void setIdxIov(
    DataType *bigint,
    DataType *int_type,
    long long key,
    long long *keybuf,
    unsigned int id,
    unsigned int *idbuf,
    std::vector<struct iovec>& iov)
{
    *keybuf = key;
    *idbuf = id;
    bigint->htobe(keybuf);
    int_type->htobe(idbuf);
    iov[0].iov_base = keybuf;
    iov[0].iov_len = sizeof(long long);
    iov[1].iov_base = idbuf;
    iov[1].iov_len = sizeof(unsigned int);
}

bool createBlock(
    Table *table,
    unsigned int blockid,
    unsigned int next,
    unsigned short type,
    std::vector<std::vector<struct iovec>> &iovs)
{
    unsigned int selfid = table->allocate();
    REQUIRE(selfid == blockid);

    DataBlock data;
    data.setTable(table);
    BufDesp *bd = kBuffer.borrow("table", blockid);
    REQUIRE(bd);
    data.attach(bd->buffer);
    data.setType(type);
    data.setNext(next);

    for (int i = 0; i < iovs.size(); i++) {
        auto ret = data.insertRecord(iovs[i]);
        if (!ret.first) return false;
    }
    kBuffer.releaseBuf(bd);
    return true;
}

TEST_CASE("IndexTest", "[p2]") 
{
    SECTION("search")
    {
        Table table;
        REQUIRE(table.open("table") == S_OK);
        table.deallocate(1);

        DataType *bigint = findDataType("BIGINT");
        DataType *int_type = findDataType("INT");
        std::vector<std::vector<struct iovec>> iovs;
        std::vector<struct iovec> iov(2);
        long long keys[20];
        unsigned int blockids[20];

        SuperBlock super;
        BufDesp *bd = kBuffer.borrow("table", 0);
        REQUIRE(bd);
        super.attach(bd->buffer);
        super.setRoot(1);
        kBuffer.releaseBuf(bd);

        // 第1个节点（根）
        setIdxIov(bigint, int_type, 13, &keys[0], 3, &blockids[0], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 1, 2, BLOCK_TYPE_INDEX, iovs));
        iovs.clear();

        // 第2个节点
        setIdxIov(bigint, int_type, 7, &keys[1], 5, &blockids[1], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 2, 4, BLOCK_TYPE_INDEX, iovs));
        iovs.clear();

        // 第3个
        setIdxIov(bigint,int_type, 23, &keys[2], 7, &blockids[2], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 31, &keys[3], 8, &blockids[3], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 43, &keys[4], 9, &blockids[4], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 3, 6, BLOCK_TYPE_INDEX, iovs));
        iovs.clear();

        // 第4个
        // 第4到9均为叶节点
        setIdxIov(bigint, int_type, 2, &keys[5], 20, &blockids[5], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 3, &keys[6], 30, &blockids[6], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 5, &keys[7], 50, &blockids[7], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 4, 5, BLOCK_TYPE_DATA, iovs));
        iovs.clear();

        // 第5个
        setIdxIov(bigint, int_type, 7, &keys[8], 70, &blockids[8], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 11, &keys[9], 110, &blockids[9], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 5, 6, BLOCK_TYPE_DATA, iovs));
        iovs.clear();

        // 第6个
        setIdxIov(bigint, int_type, 13, &keys[10], 130, &blockids[10], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 17, &keys[11], 170, &blockids[11], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 19, &keys[12], 190, &blockids[12], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 6, 7, BLOCK_TYPE_DATA, iovs));
        iovs.clear();

        // 第7个
        setIdxIov(bigint, int_type, 23, &keys[13], 230, &blockids[13], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 29, &keys[14], 290, &blockids[14], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 7, 8, BLOCK_TYPE_DATA, iovs));
        iovs.clear();

        // 第8个
        setIdxIov(bigint, int_type, 31, &keys[15], 310, &blockids[15], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 37, &keys[16], 370, &blockids[16], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 41, &keys[17], 410, &blockids[17], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 8, 9, BLOCK_TYPE_DATA, iovs));
        iovs.clear();

        // 第9个
        setIdxIov(bigint, int_type, 43, &keys[18], 430, &blockids[18], iov);
        iovs.push_back(iov);
        setIdxIov(bigint, int_type, 47, &keys[19], 470, &blockids[19], iov);
        iovs.push_back(iov);
        REQUIRE(createBlock(&table, 9, NULL, BLOCK_TYPE_DATA, iovs));
        iovs.clear();

        DataBlock data;
        data.setTable(&table);
        bd = kBuffer.borrow("table", 1);
        data.attach(bd->buffer);

        // 检查13
        // 当根节点有所查键
        long long key = 13;
        bigint->htobe(&key);
        REQUIRE(data.search(&key, sizeof(long long), iov) == S_OK);
        bigint->betoh(iov[0].iov_base);
        int_type->betoh(iov[1].iov_base);
        REQUIRE(*(long long *) iov[0].iov_base == 13); // 检查记录的键
        REQUIRE(*(unsigned int *) iov[1].iov_base == 130); // 检查记录的值

        // 检查43
        // 当内节点有所查键
        key = 43;
        bigint->htobe(&key);
        REQUIRE(data.search(&key, sizeof(long long), iov) == S_OK);
        bigint->betoh(iov[0].iov_base);
        int_type->betoh(iov[1].iov_base);
        REQUIRE(*(long long *) iov[0].iov_base == 43);
        REQUIRE(*(unsigned int *) iov[1].iov_base == 430);

        // 检查37
        // 当内节点无所查键
        key = 37;
        bigint->htobe(&key);
        REQUIRE(data.search(&key, sizeof(long long), iov) == S_OK);
        bigint->betoh(iov[0].iov_base);
        int_type->betoh(iov[1].iov_base);
        REQUIRE(*(long long *) iov[0].iov_base == 37);
        REQUIRE(*(unsigned int *) iov[1].iov_base == 370);

        // 检查2
        // 最小的键
        key = 2;
        bigint->htobe(&key);
        REQUIRE(data.search(&key, sizeof(long long), iov) == S_OK);
        bigint->betoh(iov[0].iov_base);
        int_type->betoh(iov[1].iov_base);
        REQUIRE(*(long long *) iov[0].iov_base == 2);
        REQUIRE(*(unsigned int *) iov[1].iov_base == 20);

        // 检查47
        // 最大的键
        key = 47;
        bigint->htobe(&key);
        REQUIRE(data.search(&key, sizeof(long long), iov) == S_OK);
        bigint->betoh(iov[0].iov_base);
        int_type->betoh(iov[1].iov_base);
        REQUIRE(*(long long *) iov[0].iov_base == 47);
        REQUIRE(*(unsigned int *) iov[1].iov_base == 470);

        // 检查键不存在
        key = 12;
        bigint->htobe(&key);
        REQUIRE(data.search(&key, sizeof(long long), iov) == EFAULT);

        kBuffer.releaseBuf(bd);
    }

    SECTION("insert")
    {
        Table table;
        REQUIRE(table.open("table") == S_OK);

        DataType *bigint = findDataType("BIGINT");
        DataType *int_type = findDataType("INT");
        std::vector<struct iovec> iov(2);

        DataBlock data;
        data.setTable(&table);
        BufDesp *bd = kBuffer.borrow("table", 1);
        REQUIRE(bd);
        data.attach(bd->buffer);

        long long preKeys[] = {
            2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47};
        
        // 先初始化 iov 为任意值
        long long tmpKey;
        unsigned int tmpVal;
        setIdxIov(bigint, int_type, -1, &tmpKey, -1, &tmpVal, iov);

        // 检查现有的B+树
        for (int i = 0; i < 15; ++i) {
            bigint->htobe(&preKeys[i]);
            REQUIRE(data.search(&preKeys[i], sizeof(long long), iov) == S_OK);
        }

        // 大规模插入
        std::vector<long long> keys = { 1, 8, 12, 15, 22, 30, 33, 44, 46, 48 };
        std::vector<unsigned int> vals = { 10, 80, 120, 150, 220, 300, 330, 440, 460, 480 };

        for (int i = 50; i < 2051; i += 2) {
            keys.push_back(i);
            vals.push_back(i * 10);
        }

        for (int i = 0; i < keys.size(); ++i) {
            setIdxIov(
                bigint, int_type, keys[i], &keys[i], vals[i], &vals[i], iov);
            REQUIRE(data.insert(iov) == S_OK);
        }
        
        // 检查插入
        for (int i = 0; i < keys.size(); ++i) {
            REQUIRE(data.search(&keys[i], sizeof(long long), iov) == S_OK);
            REQUIRE(*(unsigned int *) iov[1].iov_base == vals[i]);
        }       

        kBuffer.releaseBuf(bd);
    }

    SECTION("remove")
    {
        Table table;
        REQUIRE(table.open("table") == S_OK);

        DataType *bigint = findDataType("BIGINT");
        DataType *int_type = findDataType("INT");
        std::vector<struct iovec> iov(2);

        DataBlock data;
        BufDesp *bd = nullptr;
        data.setTable(&table);
        data.attachBuffer(&bd, 1);

        // 先初始化 iov 为任意值
        long long tmpKey;
        unsigned int tmpVal;
        setIdxIov(bigint, int_type, -1, &tmpKey, -1, &tmpVal, iov);

        // 上一 SECTION 中的键
        std::vector<long long> preKeys = {1, 8, 12, 15, 22, 30, 33, 44, 46, 48};
        for (int i = 50; i < 2051; i += 2)
            preKeys.push_back(i);

        // 检查现有B+树
        for (int i = 0; i < preKeys.size(); ++i) {
            bigint->htobe(&preKeys[i]);
            REQUIRE(data.search(&preKeys[i], sizeof(long long), iov) == S_OK);
        }

        // 检查删除
        for (int i = 0; i < preKeys.size(); ++i) {
            tmpKey = preKeys[i];
            tmpVal = (unsigned int) preKeys[i] * 10;
            REQUIRE(data.remove(iov) == S_OK);
        }       

        kBuffer.releaseBuf(bd);
    }

    SECTION("update")
    {
        Table table;
        REQUIRE(table.open("table") == S_OK);

        DataType *bigint = findDataType("BIGINT");
        DataType *intType = findDataType("INT");
        std::vector<struct iovec> iov(2);

        DataBlock data;
        BufDesp *bd = nullptr;
        data.setTable(&table);
        data.attachBuffer(&bd, 1);

        // 先初始化 iov 为任意值
        long long key;
        unsigned int val;
        setIdxIov(bigint, intType, -1, &key, -1, &val, iov);

        // B+树中剩余的键
        std::vector<long long> preKeys = {
            2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47};
        
        // 存放记录第2个字段原来的值
        std::vector<unsigned int> preVals = {
            20, 30, 50, 70, 110, 130, 170, 190, 230, 290, 310, 370, 410, 430, 470};

        // 大规模插入，分裂为多个 block 后再检查更新
        for (int i = 50; i < 1551; i += 2) {           
            key = (long long) i;
            val = (unsigned int) i * 10;
            preKeys.push_back(key);
            preVals.push_back(val);
            bigint->htobe(&key);
            intType->htobe(&val);
            REQUIRE(data.insert(iov) == S_OK);
        }

        for (int i = 0; i < preKeys.size(); ++i) {           
            bigint->htobe(&preKeys[i]); // 先确认键存在
            REQUIRE(
                data.search(&preKeys[i], (unsigned int) sizeof(long long), iov) ==
                S_OK);
            
            // 将记录第2个字段的值均更新为2倍
            val = preVals[i] * 2;
            intType->htobe(&val);
            REQUIRE(data.update(iov) == S_OK);            
        }

        // 检查更新
        for (int i = 0; i < preKeys.size(); ++i) {
            REQUIRE(
                data.search(&preKeys[i], (unsigned int) sizeof(long long), iov) ==
                S_OK);
            intType->betoh(&val);
            REQUIRE(val == preVals[i] * 2);
        }      
        kBuffer.releaseBuf(bd);
    }
}