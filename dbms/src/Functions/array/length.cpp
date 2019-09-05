#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>


namespace DB
{


/** Calculates the length of a string in bytes.
  */
struct LengthImpl
{
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = offsets[i] - 1 - offsets[i - 1];
    }

    static void vector_fixed_to_constant(const ColumnString::Chars & /*data*/, size_t n, UInt64 & res)
    {
        res = n;
    }

    static void vector_fixed_to_vector(const ColumnString::Chars & /*data*/, size_t /*n*/, PaddedPODArray<UInt64> & /*res*/)
    {
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = offsets[i] - offsets[i - 1];
    }
};


struct NameLength
{
    static constexpr auto name = "length";
};

using FunctionLength = FunctionStringOrArrayToT<LengthImpl, NameLength, UInt64>;

struct BloomFilterHashImpl
{
    static constexpr auto is_fixed_to_constant = true;

    static void getResults(const ColumnPtr column, PaddedPODArray<UInt64> & res)
    {
				if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
				{
						size_t size = col->size(); 
            res.resize(size);

						for (size_t i = 0; i < size; ++i)
						{
								const StringRef str = col->getDataAt(i);
								res[i] = getHash(str.data, str.size);
						}
				}
				else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
				{
						size_t size = col_fixed->size(); 
            res.resize(size);

						const size_t length = col_fixed->getN();
						const auto & data = col_fixed->getChars();
						for (size_t i = 0; i < size; ++i)
						{
								res[i] = getHash(reinterpret_cast<const char *>(&data[i * length]), length);
						}
				}
				else if (checkColumn<ColumnNullable>(column.get()))
				{
						res.resize(1);
						res[0] = 0;
				}
				else if (const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(column.get()))
				{
						const ColumnString * col_nested = nullptr;

						const ColumnNullable *nullable = checkAndGetColumn<ColumnNullable>(col_array->getData());
						if (nullable)
						{
								col_nested = checkAndGetColumn<ColumnString>(nullable->getNestedColumnPtr().get());
						}
						else
						{
								col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());
						}

						if (col_nested)
						{
								size_t size = col_array->size(); 
								res.resize(size);

								const ColumnString::Chars & data = col_nested->getChars();

								ColumnArray::Offset current_offset = 0;
								const ColumnArray::Offsets & offsets = col_array->getOffsets();
								const ColumnString::Offsets & string_offsets = col_nested->getOffsets();
								for (size_t i = 0; i < size; ++i)
								{
										const auto array_size = offsets[i] - current_offset;
										UInt64 hash = 0;

										for (size_t j = 0; j < array_size; ++j)
										{
												ColumnArray::Offset string_pos = current_offset == 0 && j == 0
														? 0
														: string_offsets[current_offset + j - 1];

												ColumnArray::Offset string_size = string_offsets[current_offset + j] - string_pos - 1;

												hash |= getHash(reinterpret_cast<const char*>(&data[string_pos]), string_size);
										}

										res[i] = hash;
										current_offset = offsets[i];
								}
						}
				}
    }

		static constexpr UInt64 SEED = 3701;
		static constexpr UInt64 SEED_GEN_A = 845897321;
		static constexpr UInt64 SEED_GEN_B = 217728422;
		static UInt64 getHash(const char * s, size_t len)
		{
				UInt64 hash;

				UInt64 h1 = CityHash_v1_0_2::CityHash64WithSeed(s, len, SEED);
				//UInt64 h2 = CityHash_v1_0_2::CityHash64WithSeed(s, len, SEED_GEN_A * SEED + SEED_GEN_B);

				hash = (1ULL << h1 % 64);
				//hash |= (1ULL << (h1 + h2 + 1) % 64);

				return hash;
		}
};

struct NameBloomFilterHash
{
    static constexpr auto name = "bloomFilterHash2";
};

using FunctionBloomFilter = FunctionStringOrStringArrayToT<BloomFilterHashImpl, NameBloomFilterHash, UInt64>;

void registerFunctionLength(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLength>(FunctionFactory::CaseInsensitive);
    // TODO
    factory.registerFunction<FunctionBloomFilter>(FunctionFactory::CaseInsensitive);
}

}
