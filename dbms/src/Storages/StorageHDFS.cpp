#include <Common/config.h>

#if USE_HDFS

#include <Storages/StorageFactory.h>
#include <Storages/StorageHDFS.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadBufferFromHDFS.h>
#include <IO/WriteBufferFromHDFS.h>
#include <IO/HDFSCommon.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <Common/parseGlobs.h>
#include <Poco/URI.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <hdfs/hdfs.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

StorageHDFS::StorageHDFS(const String & uri_,
    const std::string & database_name_,
    const std::string & table_name_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    Context & context_)
    : IStorage(columns_)
    , uri(uri_)
    , format_name(format_name_)
    , table_name(table_name_)
    , database_name(database_name_)
    , context(context_)
{
}

namespace
{

class HDFSBlockInputStream : public IBlockInputStream
{
public:
    HDFSBlockInputStream(const String & uri,
        const String & format,
        const Block & sample_block,
        const Context & context,
        UInt64 max_block_size)
    {
        std::unique_ptr<ReadBuffer> read_buf = std::make_unique<ReadBufferFromHDFS>(uri);
        auto input_stream = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);
        reader = std::make_shared<OwningBlockInputStream<ReadBuffer>>(input_stream, std::move(read_buf));
    }

    String getName() const override
    {
        return "HDFS";
    }

    Block readImpl() override
    {
        return reader->read();
    }

    Block getHeader() const override
    {
        return reader->getHeader();
    }

    void readPrefixImpl() override
    {
        reader->readPrefix();
    }

    void readSuffixImpl() override
    {
        reader->readSuffix();
    }

private:
    BlockInputStreamPtr reader;
};

class HDFSBlockOutputStream : public IBlockOutputStream
{
public:
    HDFSBlockOutputStream(const String & uri,
        const String & format,
        const Block & sample_block_,
        const Context & context)
        : sample_block(sample_block_)
    {
        write_buf = std::make_unique<WriteBufferFromHDFS>(uri);
        writer = FormatFactory::instance().getOutput(format, *write_buf, sample_block, context);
    }

    Block getHeader() const override
    {
        return sample_block;
    }

    void write(const Block & block) override
    {
        writer->write(block);
    }

    void writePrefix() override
    {
        writer->writePrefix();
    }

    void writeSuffix() override
    {
        writer->writeSuffix();
        writer->flush();
        write_buf->sync();
    }

private:
    Block sample_block;
    std::unique_ptr<WriteBufferFromHDFS> write_buf;
    BlockOutputStreamPtr writer;
};

//static Strings recursiveLSWithRegexpMatching(const String & cur_path, hdfsFS fs, const re2::RE2 & matcher)
//{
//    HDFSFileInfo ls;
//    ls.file_info = hdfsListDirectory(fs.get(), path_without_globs, ls.length);
//
//}

}


BlockInputStreams StorageHDFS::read(
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context_,
    QueryProcessingStage::Enum  /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    Strings path_parts;
    size_t first_glob = uri.find_first_of("*?{");

    if (first_glob == std::string::npos)
        return {std::make_shared<HDFSBlockInputStream>(
                uri,
                format_name,
                getSampleBlock(),
                context_,
                max_block_size)};

    String uri_without_globs = uri.substr(0, first_glob);
    size_t end_of_path_without_globs = uri_without_globs.rfind('/');
    uri_without_globs = uri_without_globs.substr(0, end_of_path_without_globs + 1);

    size_t begin_of_path = uri.find('/', uri.find("//") + 2);
    String path_from_uri = uri.substr(begin_of_path);
    String uri_without_path = uri.substr(0, begin_of_path);

    String path_pattern = makeRegexpPatternFromGlobs(path_from_uri);
    re2::RE2 matcher(path_pattern);

    HDFSBuilderPtr builder = createHDFSBuilder(Poco::URI(uri_without_globs));
    HDFSFSPtr fs = createHDFSFS(builder.get());
//    Strings res_paths = recursiveLSWithRegexpMatching(path_without_globs, fs.get(), matcher);

    HDFSFileInfo ls;
    String path_without_globs = uri_without_globs.substr(begin_of_path);
    ls.file_info = hdfsListDirectory(fs.get(), path_without_globs.data(), &ls.length);
    BlockInputStreams result;
    for (int i = 0; i < ls.length; ++i)
    {
        if (ls.file_info[i].mKind == 'F')
        {
            String cur_path = String(ls.file_info[i].mName);
            if (cur_path[1] == '/')
            {
                cur_path = cur_path.substr(1);
            }

            if (re2::RE2::FullMatch(cur_path, matcher))
            {
                result.push_back(
                        std::make_shared<HDFSBlockInputStream>(uri_without_path + String(ls.file_info[i].mName), format_name, getSampleBlock(), context_,
                                                               max_block_size));
            }
        }
    }

    return result;
}

void StorageHDFS::rename(const String & /*new_path_to_db*/, const String & new_database_name, const String & new_table_name)
{
    table_name = new_table_name;
    database_name = new_database_name;
}

BlockOutputStreamPtr StorageHDFS::write(const ASTPtr & /*query*/, const Context & /*context*/)
{
    return std::make_shared<HDFSBlockOutputStream>(uri, format_name, getSampleBlock(), context);
}

void registerStorageHDFS(StorageFactory & factory)
{
    factory.registerStorage("HDFS", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (!(engine_args.size() == 1 || engine_args.size() == 2))
            throw Exception(
                "Storage HDFS requires exactly 2 arguments: url and name of used format.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        String format_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageHDFS::create(url, args.database_name, args.table_name, format_name, args.columns, args.context);
    });
}

}

#endif
