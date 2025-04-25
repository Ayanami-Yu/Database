// buffer 单元测试
#include "../catch.hpp"
#include <db/buffer.h>
#include <db/file.h>
#include <db/block.h>
using namespace db;

TEST_CASE("db/buffer.h", "[p1][p2]")
{
    SECTION("init")
    {
        kBuffer.init(&kFiles);
        REQUIRE(kBuffer.idles() == 256 * 1024 * 1024 / BLOCK_SIZE);

        BufDesp *bd = kBuffer.borrow(Schema::META_FILE, 0);
        REQUIRE(bd);
        REQUIRE(bd->buffer);
        REQUIRE(bd->ref.load() == 1);
        kBuffer.releaseBuf(bd);
        REQUIRE(bd->ref.load() == 0);
    }
}