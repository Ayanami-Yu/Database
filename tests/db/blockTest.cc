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

inline void htobe_iov(
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

        // 检查 block，table 表是空的，未添加任何表项
        // REQUIRE(data.checksum());
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
        htobe_iov(bigint, char_type, varchar, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);        
        unsigned short osize = data.getFreespaceSize();
        unsigned short nsize = data.requireLength(iov);

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
        htobe_iov(bigint, char_type, varchar, &nid, phone, addr);
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
        htobe_iov(bigint, char_type, varchar, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(!data.updateRecord(iov));
       
        // 使更新后的记录更长
        addr = LONG_ADDR;
        bigint->betoh(&nid);
        nid -= 1;
        htobe_iov(bigint, char_type, varchar, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        REQUIRE(!data.getNext());  // 检查此时没有新 block
        REQUIRE(data.updateRecord(iov));
       
        kBuffer.writeBuf(bd);
        kBuffer.releaseBuf(bd);

        // 检查分裂出新 block
        unsigned int blockid = data.getNext();
        bd = kBuffer.borrow("table", blockid);
        REQUIRE(bd);
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
        htobe_iov(bigint, nullptr, nullptr, &nid, phone, addr);
        setIov(iov, &nid, phone, (void *) addr);
        data.clear(1, 1, BLOCK_TYPE_DATA);
        REQUIRE(!data.removeRecord(iov));

        // 测试记录存在
        REQUIRE(data.insertRecord(iov).first == true);
        REQUIRE(data.removeRecord(iov));

        kBuffer.releaseBuf(bd);
    }
}

inline void setIdxIov(
    long long* key,
    unsigned int* blockid,
    std::vector<struct iovec>& iov)
{
    iov[0].iov_base = key;
    iov[0].iov_len = sizeof(long long);
    iov[1].iov_base = blockid;
    iov[1].iov_len = sizeof(unsigned int);
}

TEST_CASE("IndexTest", "[p2]") 
{
    SECTION("search")
    {
        Table table;
        REQUIRE(table.open("table") == S_OK);

        DataBlock data;
        data.setTable(&table);
        BufDesp *bd = kBuffer.borrow("table", 1);
        REQUIRE(bd);
        data.attach(bd->buffer);

        DataType *bigint = findDataType("BIGINT");
        std::vector<struct iovec> iov(2);
        long long key;
        unsigned int blockid;

        key = 10;
        blockid = 1;
        setIdxIov(&key, &blockid, iov);

        printf("fs = %hu", data.getFreeSize());
        
        kBuffer.releaseBuf(bd);
    }
}