#include "mdbx.h++"

#include <iostream>
#include <random>
#include <vector>

#define NN 10000

std::string format_va(const char *fmt, va_list ap) {
  va_list ones;
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  std::string result;
  result.reserve(size_t(needed + 1));
  result.resize(size_t(needed), '\0');
  assert(int(result.capacity()) > needed);
  int actual = vsnprintf(const_cast<char *>(result.data()), result.capacity(),
                         fmt, ones);
  assert(actual == needed);
  (void)actual;
  va_end(ones);
  return result;
}

std::string format(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  std::string result = format_va(fmt, ap);
  va_end(ap);
  return result;
}

struct acase {
  unsigned klen_min, klen_max;
  unsigned vlen_min, vlen_max;
  unsigned dupmax_log2;
};

// std::random_device rd;
std::mt19937_64 rnd;

static unsigned rnd_len(unsigned min, unsigned max) {
  return (min < max) ? min + rnd() % (max - min) : min;
}

static mdbx::slice mk(mdbx::default_buffer &buf, unsigned len) {
  buf.clear_and_reserve(len);
  for (unsigned i = 0; i < len; ++i)
    buf.append_byte(rnd());
  return buf.slice();
}

static std::string name(unsigned n) { return format("Commitment_%05u", n); }

static mdbx::map_handle create_and_fill(mdbx::txn txn, const acase &thecase,
                                        const unsigned n) {
  auto map = txn.create_map(name(n),
                            (thecase.klen_min == thecase.klen_max &&
                             (thecase.klen_min == 4 || thecase.klen_max == 8))
                                ? mdbx::key_mode::ordinal
                                : mdbx::key_mode::usual,
                            (thecase.vlen_min == thecase.vlen_max)
                                ? mdbx::value_mode::multi_samelength
                                : mdbx::value_mode::multi);
  mdbx::buffer k, v;
  for (auto i = 0u; i < NN; i++) {
    mk(k, rnd_len(thecase.klen_min, thecase.klen_min));
    for (auto ii = thecase.dupmax_log2
                       ? 1u + (rnd() & ((2u << thecase.dupmax_log2) - 1u))
                       : 1u;
         ii > 0; --ii)
      txn.upsert(map, k, mk(v, rnd_len(thecase.vlen_min, thecase.vlen_min)));
  }
  return map;
}

static void chunched_delete(mdbx::txn txn, const acase &thecase,
                            const unsigned n) {
  mdbx::buffer r;
  auto map = txn.open_map_accede(name(n));
  auto cursor = txn.open_cursor(map);
  for (size_t n = 0; n < 10; ++n) {
    mk(r, rnd_len(thecase.klen_min, thecase.klen_min));
    if (cursor.lower_bound(r))
      do
        cursor.erase();
      while (cursor.to_next(false));
  }

  if (cursor.to_first(false))
    do
      cursor.erase();
    while (cursor.to_next(false));
}

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;

  mdbx::env::remove(".");

  std::vector<acase> testset;
  // Там ключи разной длины - от 1 до 64 байт. Значения разной длины от 100 до
  // 1000 байт.
  testset.emplace_back(/* keylen_min */ 1, /* keylen_max */ 64,
                       /* datalen_min */ 100, /* datalen_max */ 4000,
                       /* dups_log2 */ 10);
  // В одной таблице DupSort: path -> version_u64+data
  // path - это префикс в дереве. Самые частые длины: 1-5 байт и 32-36 байт.
  testset.emplace_back(1, 5, 100, 1000, 12);
  testset.emplace_back(32, 36, 100, 1000, 12);
  // В другой DupSort: timestamp_u64 -> path
  testset.emplace_back(8, 8, 1, 5, 12);
  testset.emplace_back(8, 8, 32, 36, 12);

  mdbx::env_managed env(".", mdbx::env_managed::create_parameters(),
                        mdbx::env::operate_parameters(42));

  auto txn = env.start_write();
  for (unsigned i = 0; i < testset.size(); ++i)
    create_and_fill(txn, testset[i], i);
  txn.commit();

  txn = env.start_write();
  for (unsigned i = 0; i < testset.size(); ++i)
    chunched_delete(txn, testset[i], i);
  txn.commit();

  std::cout << "OK\n";
  return EXIT_SUCCESS;
}
