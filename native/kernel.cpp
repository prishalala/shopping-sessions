// Shopping Sessions: the complete pipeline in one static binary.
//
// Phases
//   0. Map every input file and cut it into LF-terminated chunks.
//   1. Links: intern IDs in a lock-free table, union-find, name each person by
//      the byte-wise smallest ID.
//   2. One parallel pass over events and clicks. Valid events and clicks go to
//      person partitions. Every well-formed event with an ID also records its
//      dedup-bitmap slot and line in an event-ID partition.
//   3. Per event-ID partition: find bitmap slots seen more than once. Those
//      "contested" events are resolved exactly (full key, earliest ingest_ts,
//      validation of the kept copy) and the valid winners are re-emitted.
//   4. Per person partition: drop contested rows, add winners, sort by person
//      and time, sessionize, attribute, aggregate.
//   5. Write Parquet directly.
#include <dirent.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <new>

using Text = std::string_view;
using Time = int64_t;
constexpr Time DAY = 86400000, WEEK = 7 * DAY, GAP = 1800000;
constexpr Time BAD = INT64_MIN;
constexpr uint32_t NONE = UINT32_MAX;

static int T = 8;

[[noreturn]] static void die(const char* what, const std::string& detail = {}) {
  fprintf(stderr, "fatal: %s %s\n", what, detail.c_str());
  exit(1);
}

static double now() {
  return std::chrono::duration<double>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
static double t_start;
static bool verbose = true;
static void mark(const char* what) {
  if (verbose) fprintf(stderr, "%8.3f  %s\n", now() - t_start, what);
}

template <class F>
static void parallel(size_t jobs, F&& fn) {
  std::atomic<size_t> next{0};
  auto work = [&](int w) {
    for (size_t i; (i = next.fetch_add(1, std::memory_order_relaxed)) < jobs;)
      fn(w, i);
  };
  int n = int(std::min<size_t>(T, jobs));
  std::vector<std::thread> pool;
  for (int w = 1; w < n; ++w) pool.emplace_back(work, w);
  if (n > 0) work(0);
  for (auto& t : pool) t.join();
}

static void* alloc_zero(size_t bytes) {
  if (!bytes) bytes = 1;
  void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (p == MAP_FAILED) die("mmap anonymous");
  madvise(p, bytes, MADV_HUGEPAGE);
  return p;
}

// Bump arena for the big record buffers: one huge-page-advised virtual region,
// carved into per-thread slabs. Freeing is a no-op; the process exits soon.
static char* arena_base;
static std::atomic<size_t> arena_used{0};
static constexpr size_t ARENA = size_t(96) << 30, SLAB = size_t(64) << 20;
static thread_local char* slab_at = nullptr;
static thread_local char* slab_end = nullptr;
static void* arena_take(size_t bytes) {
  bytes = (bytes + 63) & ~size_t(63);
  if (size_t(slab_end - slab_at) < bytes) {
    size_t want = std::max(bytes, SLAB);
    size_t off = arena_used.fetch_add(want);
    if (off + want > ARENA) die("arena exhausted");
    slab_at = arena_base + off;
    slab_end = slab_at + want;
  }
  void* p = slab_at;
  slab_at += bytes;
  return p;
}
template <class T>
struct ArenaAlloc {
  using value_type = T;
  ArenaAlloc() = default;
  template <class U>
  ArenaAlloc(const ArenaAlloc<U>&) {}
  T* allocate(size_t n) { return static_cast<T*>(arena_take(n * sizeof(T))); }
  void deallocate(T*, size_t) {}
  template <class U>
  bool operator==(const ArenaAlloc<U>&) const { return true; }
  template <class U>
  bool operator!=(const ArenaAlloc<U>&) const { return false; }
};
template <class T>
using AVec = std::vector<T, ArenaAlloc<T>>;

// ---------------------------------------------------------------- parsing

inline Time decimal(Text s, Time limit) {
  if (s.empty()) return BAD;
  // All bytes must be ASCII digits; leading zeros are free. Up to 18
  // significant digits cannot overflow; longer values are checked exactly.
  size_t i = 0, n = s.size();
  while (i < n && s[i] == '0') ++i;
  uint64_t value = 0;
  if (n - i <= 18) {
    for (; i < n; ++i) {
      unsigned digit = unsigned(uint8_t(s[i])) - '0';
      if (digit > 9) return BAD;
      value = value * 10 + digit;
    }
    return Time(value) > limit ? BAD : Time(value);
  }
  for (size_t k = i; k < n; ++k)
    if (unsigned(uint8_t(s[k])) - '0' > 9) return BAD;
  if (n - i > 19) return BAD;  // more than 19 significant digits overflows
  for (; i < n; ++i) value = value * 10 + (uint8_t(s[i]) - '0');
  // 19 digits: overflow beyond uint64 impossible (< 1e19 < 2^64)
  return value > uint64_t(limit) ? BAD : Time(value);
}

inline int two(const char* s) {
  unsigned a = static_cast<unsigned char>(s[0]) - unsigned('0');
  unsigned b = static_cast<unsigned char>(s[1]) - unsigned('0');
  return (a > 9 || b > 9) ? -10000 : int(a * 10 + b);
}

inline Time timestamp(Text s) {
  if (!((s.size() == 20 || s.size() == 25) && s[4] == '-'))
    return decimal(s, INT64_MAX);
  if (s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return BAD;
  const char* p = s.data();
  int y = two(p) * 100 + two(p + 2), m = two(p + 5), d = two(p + 8);
  int hour = two(p + 11), minute = two(p + 14), sec = two(p + 17);
  if (y < 1 || m < 1 || m > 12 || d < 1 || hour < 0 || hour > 23 ||
      minute < 0 || minute > 59 || sec < 0 || sec > 59)
    return BAD;
  static constexpr int lengths[] = {31, 28, 31, 30, 31, 30,
                                    31, 31, 30, 31, 30, 31};
  bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
  if (d > lengths[m - 1] + (m == 2 && leap)) return BAD;
  int offset = 0;
  if (s.size() == 20) {
    if (s[19] != 'Z') return BAD;
  } else {
    if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return BAD;
    int h = two(p + 20), n = two(p + 23);
    if (h < 0 || h > 23 || n < 0 || n > 59) return BAD;
    offset = (h * 60 + n) * (s[19] == '+' ? 1 : -1);
  }
  y -= m <= 2;
  int era = y / 400, yr = y % 400;
  int days = era * 146097 + yr * 365 + yr / 4 - yr / 100 +
             (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1 - 719468;
  return days * DAY + ((hour * 60 + minute - offset) * 60 + sec) * 1000LL;
}

// Byte classes: 1 comma, 2 quote, 3 LF.
static uint8_t cls[256];
static void init_cls() {
  cls[uint8_t(',')] = 1;
  cls[uint8_t('"')] = 2;
  cls[uint8_t('\n')] = 3;
}

// Split one LF-terminated line into exactly N fields. Returns the next line.
// Reads up to 63 bytes past the LF; every chunk is followed by >= 64 readable
// bytes (see add_mapped).
#include <immintrin.h>
template <int N>
static inline const char* split(const char* p, Text* f, bool& ok) {
  const __m256i comma = _mm256_set1_epi8(','), quote = _mm256_set1_epi8('"'),
                lf = _mm256_set1_epi8('\n');
  const char* fs = p;  // field start
  int n = 0, q = 0;
  bool bad = false;
  auto finish = [&](const char* end) {
    if (n < N) {
      if (q) {
        if (q == 2 && end - fs >= 2 && *fs == '"' && end[-1] == '"')
          f[n] = Text(fs + 1, size_t(end - fs - 2));
        else
          bad = true;
      } else {
        f[n] = Text(fs, size_t(end - fs));
      }
    }
    ++n;
    q = 0;
    fs = end + 1;
  };
  for (const char* base = p;; base += 64) {
    __m256i a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base));
    __m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base + 32));
    uint64_t mc = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, comma))) |
                  (uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, comma)))) << 32);
    uint64_t mq = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, quote))) |
                  (uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, quote)))) << 32);
    uint64_t ml = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, lf))) |
                  (uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, lf)))) << 32);
    uint64_t below = ml ? ((ml & -ml) - 1) : ~uint64_t(0);
    mc &= below;
    mq &= below;
    while (mc) {
      int pos = __builtin_ctzll(mc);
      uint64_t lower = (uint64_t(1) << pos) - 1;
      q += __builtin_popcountll(mq & lower);
      mq &= ~lower;
      finish(base + pos);
      mc &= mc - 1;
    }
    q += __builtin_popcountll(mq);
    if (ml) {
      const char* end = base + __builtin_ctzll(ml);
      finish(end);
      ok = !bad && n == N;
      return end + 1;
    }
  }
}

// ---------------------------------------------------------------- hashing

static inline uint64_t ld64(const char* p) {
  uint64_t v;
  memcpy(&v, p, 8);
  return v;
}
static inline uint64_t ld32(const char* p) {
  uint32_t v;
  memcpy(&v, p, 4);
  return v;
}
static inline uint64_t mum(uint64_t a, uint64_t b) {
  __uint128_t r = (__uint128_t)a * b;
  return uint64_t(r) ^ uint64_t(r >> 64);
}
static inline uint64_t hash_text(Text s) {
  const char* p = s.data();
  size_t n = s.size();
  uint64_t a, b;
  if (n <= 16) {
    if (n >= 8) {
      a = ld64(p);
      b = ld64(p + n - 8);
    } else if (n >= 4) {
      a = ld32(p);
      b = ld32(p + n - 4);
    } else if (n) {
      a = (uint64_t(uint8_t(p[0])) << 16) | (uint64_t(uint8_t(p[n >> 1])) << 8) |
          uint8_t(p[n - 1]);
      b = 0;
    } else {
      a = b = 0;
    }
  } else {
    uint64_t h = 0x9E3779B97F4A7C15ULL;
    size_t i = 0;
    for (; i + 16 < n; i += 16)
      h = mum(ld64(p + i) ^ 0xa0761d6478bd642fULL, ld64(p + i + 8) ^ h);
    a = ld64(p + n - 16) ^ h;
    b = ld64(p + n - 8);
  }
  return mum(a ^ 0xe7037ed1a0b428dbULL ^ n, b ^ 0x8ebc6af09c88c6e3ULL);
}

// ---------------------------------------------------------------- inputs

// A byte range of one file region. Lines that START in [begin, end) belong to
// the chunk; when !aligned, begin may be mid-line and the worker skips to the
// next line start. Every line before `limit` ends with '\n' before `limit`.
struct Chunk {
  const char* begin;
  const char* end;
  const char* limit;
  bool aligned;
};
static inline const char* first_line(const Chunk& c) {
  if (c.aligned) return c.begin;
  const char* nl = static_cast<const char*>(
      memchr(c.begin - 1, '\n', size_t(c.limit - c.begin + 1)));
  return nl ? nl + 1 : c.limit;
}
struct Input {
  std::vector<Chunk> chunks;
  size_t bytes = 0;
};

static const char* map_file(const std::string& path, size_t& size) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) die("open", path);
  struct stat st;
  if (fstat(fd, &st)) die("stat", path);
  size = size_t(st.st_size);
  if (!size) {
    close(fd);
    return nullptr;
  }
  static const bool populate = getenv("KERNEL_POPULATE") != nullptr;
  void* p = mmap(nullptr, size, PROT_READ,
                 MAP_PRIVATE | (populate ? MAP_POPULATE : 0), fd, 0);

  close(fd);
  if (p == MAP_FAILED) die("mmap", path);
  return static_cast<const char*>(p);
}

// The last >= 64 bytes of each file are copied to a padded, LF-terminated heap
// buffer so that no line ever ends at the end of a mapping.
struct Mapped {
  std::string path;
  const char* data = nullptr;
  size_t size = 0;
};
static void add_mapped(Input& in, const Mapped& mf, size_t chunk_size) {
  size_t size = mf.size;
  const char* d = mf.data;
  if (!d) return;
  in.bytes += size;
  const char* end = d + size;
  const char* nl = static_cast<const char*>(memchr(d, '\n', size));
  if (!nl) return;  // header only
  const char* p = nl + 1;
  const char* tail = end - std::min<size_t>(size_t(end - p), 64);
  while (tail > p && tail[-1] != '\n') --tail;
  // Fixed byte ranges: no scanning here, so no page is touched up front.
  for (const char* lo = p; lo < tail; lo += std::min(chunk_size, size_t(tail - lo)))
    in.chunks.push_back({lo, lo + std::min(chunk_size, size_t(tail - lo)), tail, lo == p});
  if (tail < end) {
    size_t n = size_t(end - tail);
    char* buf = static_cast<char*>(malloc(n + 72));
    memcpy(buf, tail, n);
    if (buf[n - 1] != '\n') buf[n++] = '\n';
    memset(buf + n, 0, 64);
    in.chunks.push_back({buf, buf + n, buf + n, true});
  }
}

static std::vector<std::string> csv_files(const std::string& dir) {
  std::vector<std::string> out;
  DIR* dp = opendir(dir.c_str());
  if (!dp) return out;
  while (dirent* e = readdir(dp)) {
    std::string name = e->d_name;
    if (name.size() < 4 || name.compare(name.size() - 4, 4, ".csv")) continue;
    std::string path = dir + "/" + name;
    struct stat st;
    if (stat(path.c_str(), &st) || !S_ISREG(st.st_mode)) continue;
    out.push_back(path);
  }
  closedir(dp);
  std::sort(out.begin(), out.end());
  return out;
}

template <int N, class Fn>
static void each_row(const Chunk& c, Fn fn) {
  Text f[N];
  bool ok;
  for (const char* p = first_line(c); p < c.end;) {
    p = split<N>(p, f, ok);
    if (ok) fn(f);
  }
}

// ---------------------------------------------------------------- tables

// Read-only string -> index map (products, campaigns).
struct StrMap {
  struct Slot {
    Text key;
    uint32_t val;
  };
  std::vector<Slot> slots;
  size_t mask = 0;
  void build(const std::vector<std::pair<Text, uint32_t>>& items) {
    size_t cap = 16;
    while (cap < items.size() * 3) cap *= 2;
    slots.assign(cap, Slot{Text(), NONE});
    mask = cap - 1;
    for (auto& [k, v] : items) {
      size_t i = hash_text(k) & mask;
      while (slots[i].val != NONE && slots[i].key != k) i = (i + 1) & mask;
      slots[i] = {k, v};  // later duplicates win; inputs are unique anyway
    }
  }
  uint32_t find(Text k) const {
    size_t i = hash_text(k) & mask;
    for (;;) {
      const Slot& s = slots[i];
      if (s.val == NONE) return NONE;
      if (s.key == k) return s.val;
      i = (i + 1) & mask;
    }
  }
};

// Lock-free interning table for user IDs. One 32-byte slot holds the hash,
// the person, the ID number and up to 15 key bytes, so an exact hit costs one
// cache line. Longer keys fall back to comparing strs[id].
struct alignas(32) IdSlot {
  uint64_t h;  // 0 empty, 1 being written, else hash | 2
  uint32_t person, id;
  __m128i key;  // bytes [0, len) of the ID, zero padded; byte 15 = len or 255
};
static inline __m128i key_of(Text s) {
  static const struct M {
    uint8_t m[17][16];
    M() {
      for (int l = 0; l <= 16; ++l)
        for (int i = 0; i < 16; ++i) m[l][i] = i < l ? 0xFF : 0;
    }
  } masks;
  size_t n = s.size();
  __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s.data()));
  if (n <= 15) {
    v = _mm_and_si128(v, _mm_loadu_si128(reinterpret_cast<const __m128i*>(masks.m[n])));
    return _mm_or_si128(v, _mm_slli_si128(_mm_cvtsi32_si128(int(n)), 15));
  }
  v = _mm_and_si128(v, _mm_loadu_si128(reinterpret_cast<const __m128i*>(masks.m[15])));
  return _mm_or_si128(v, _mm_slli_si128(_mm_cvtsi32_si128(255), 15));
}

struct IdTable {
  IdSlot* slots = nullptr;
  size_t mask = 0;
  uint32_t cap = 0;
  Text* strs = nullptr;
  uint32_t* slot_of = nullptr;
  alignas(64) std::atomic<uint32_t> next{0};
  alignas(64) std::atomic<bool> overflow{false};
  char pad_[64];

  void init(size_t nslots) {
    slots = static_cast<IdSlot*>(alloc_zero(nslots * sizeof(IdSlot)));
    mask = nslots - 1;
    cap = uint32_t(std::min<size_t>(nslots / 2, 0xFFFFFFF0u));
    strs = static_cast<Text*>(alloc_zero(size_t(cap) * sizeof(Text)));
    slot_of = static_cast<uint32_t*>(alloc_zero(size_t(cap) * 4));
  }
  static constexpr uint32_t BLOCK = 4096;
  struct alignas(64) Cursor {
    uint32_t at = 0, end = 0;
  };
  Cursor cursors[256];
  uint32_t claim(int w) {
    Cursor& c = cursors[w];
    if (c.at == c.end) {
      uint32_t b = next.fetch_add(BLOCK, std::memory_order_relaxed);
      if (b >= cap || cap - b < BLOCK) {
        overflow.store(true);
        return NONE;
      }
      c.at = b;
      c.end = b + BLOCK;
    }
    return c.at++;
  }
  // Returns the slot of s, inserting a new singleton ID if absent.
  IdSlot* get(Text s, uint64_t hash, int w) {
    uint64_t h = hash | 2;
    __m128i k = key_of(s);
    bool longkey = s.size() > 15;
    size_t i = hash & mask;
    for (;;) {
      IdSlot* sl = slots + i;
      uint64_t x = __atomic_load_n(&sl->h, __ATOMIC_ACQUIRE);
      if (x == 0) {
        if (__atomic_compare_exchange_n(&sl->h, &x, uint64_t(1), false,
                                        __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
          uint32_t id = claim(w);
          if (id == NONE) id = 0;
          strs[id] = s;
          slot_of[id] = uint32_t(i);
          sl->person = id;
          sl->id = id;
          sl->key = k;
          __atomic_store_n(&sl->h, h, __ATOMIC_RELEASE);
          return sl;
        }
      }
      while (x == 1) {
        _mm_pause();
        x = __atomic_load_n(&sl->h, __ATOMIC_ACQUIRE);
      }
      if (x == h &&
          _mm_movemask_epi8(_mm_cmpeq_epi8(sl->key, k)) == 0xFFFF &&
          (!longkey || strs[sl->id] == s))
        return sl;
      i = (i + 1) & mask;
    }
  }
};

// ---------------------------------------------------------------- records

struct Ev {  // valid event, routed by person
  Time ts;
  uint32_t person, price, slot;
  uint16_t cat, pad;
};
// Any well-formed event with an ID, routed by dedup slot. Packed:
// bits 0-19 chunk, 20-43 offset within chunk, 44-63 local bitmap index.
struct Dd {
  uint64_t v;
  // bits 0-20 chunk, 21-40 offset within chunk, 41-63 local bitmap index
  uint32_t local() const { return uint32_t(v >> 41); }
  uint32_t chunk() const { return uint32_t(v & 0x1FFFFF); }
  uint32_t offset() const { return uint32_t((v >> 21) & 0xFFFFF); }
};
struct Ck {  // valid click, routed by person
  Time ts;
  uint32_t person, camp;
  const char* id;
  uint32_t idlen, pad;
};
// Stable LSD radix sort of (key, idx) pairs on a 32-bit key, 11/11/10 bits.
struct KI {
  uint32_t key, idx;
};
static void radix32(std::vector<KI>& a, std::vector<KI>& tmp) {
  size_t n = a.size();
  tmp.resize(n);
  static thread_local uint32_t hist[3][2048];
  memset(hist, 0, sizeof(hist));
  for (size_t i = 0; i < n; ++i) {
    uint32_t k = a[i].key;
    ++hist[0][k & 2047];
    ++hist[1][(k >> 11) & 2047];
    ++hist[2][k >> 22];
  }
  for (int d = 0; d < 3; ++d) {
    uint32_t sum = 0;
    for (int b = 0; b < 2048; ++b) {
      uint32_t c = hist[d][b];
      hist[d][b] = sum;
      sum += c;
    }
  }
  KI* src = a.data();
  KI* dst = tmp.data();
  for (int d = 0; d < 3; ++d) {
    int shift = d * 11;
    for (size_t i = 0; i < n; ++i) {
      uint32_t b = d == 2 ? (src[i].key >> 22) : ((src[i].key >> shift) & 2047);
      dst[hist[d][b]++] = src[i];
    }
    std::swap(src, dst);
  }
  if (src != a.data()) memcpy(a.data(), src, n * sizeof(KI));
}

struct SE {
  Time ts;
  uint32_t price, cat;
};
struct SC {
  Time ts;
  uint32_t camp, idlen;
  const char* id;
};

// ---------------------------------------------------------------- parquet

struct Thrift {
  std::string b;
  std::vector<int16_t> stack;
  int16_t last = 0;
  void varint(uint64_t v) {
    while (v >= 0x80) {
      b += char(v | 0x80);
      v >>= 7;
    }
    b += char(v);
  }
  void zz(int64_t v) { varint((uint64_t(v) << 1) ^ uint64_t(v >> 63)); }
  void field(int16_t id, uint8_t type) {
    int d = id - last;
    if (d > 0 && d <= 15) {
      b += char((d << 4) | type);
    } else {
      b += char(type);
      zz(id);
    }
    last = id;
  }
  void i32(int16_t id, int32_t v) {
    field(id, 5);
    zz(v);
  }
  void i64(int16_t id, int64_t v) {
    field(id, 6);
    zz(v);
  }
  void bin(Text s) {
    varint(s.size());
    b.append(s);
  }
  void str(int16_t id, Text s) {
    field(id, 8);
    bin(s);
  }
  void list(int16_t id, uint8_t etype, size_t n) {
    field(id, 9);
    if (n < 15) {
      b += char((n << 4) | etype);
    } else {
      b += char(0xF0 | etype);
      varint(n);
    }
  }
  void begin() {  // struct element or nested struct body
    stack.push_back(last);
    last = 0;
  }
  void end() {
    b += char(0);
    last = stack.back();
    stack.pop_back();
  }
  void begin_struct(int16_t id) {
    field(id, 12);
    begin();
  }
};

enum PType { INT32 = 1, INT64 = 2, BYTE_ARRAY = 6 };
enum Logical { NOLOGICAL, STRING, DATE };

struct Column {
  const char* name;
  int type;
  int logical;
  bool optional;
  std::string data;          // PLAIN values of the non-null rows
  std::vector<uint8_t> def;  // one bit per row when optional
  size_t rows = 0;
  void put_str(Text s) {
    uint32_t n = uint32_t(s.size());
    data.append(reinterpret_cast<const char*>(&n), 4);
    data.append(s);
  }
  void put_i32(int32_t v) { data.append(reinterpret_cast<const char*>(&v), 4); }
  void put_i64(int64_t v) { data.append(reinterpret_cast<const char*>(&v), 8); }
  void present(bool yes) {
    if ((rows & 7) == 0) def.push_back(0);
    if (yes) def.back() |= uint8_t(1u << (rows & 7));
    ++rows;
  }
};

static void put_le32(std::string& s, uint32_t v) {
  s.append(reinterpret_cast<const char*>(&v), 4);
}

static void write_parquet(const std::string& path, size_t rows,
                          std::vector<Column*> cols) {
  std::string out = "PAR1";
  std::vector<std::pair<int64_t, int64_t>> chunks;  // offset, size
  int64_t total = 0;
  for (Column* c : cols) {
    std::string levels;
    if (c->optional) {
      Thrift rle;
      size_t groups = (rows + 7) / 8;
      rle.varint((uint64_t(groups) << 1) | 1);
      rle.b.append(reinterpret_cast<const char*>(c->def.data()), groups);
      put_le32(levels, uint32_t(rle.b.size()));
      levels += rle.b;
    }
    size_t page = levels.size() + c->data.size();
    Thrift h;
    h.i32(1, 0);
    h.i32(2, int32_t(page));
    h.i32(3, int32_t(page));
    h.begin_struct(5);
    h.i32(1, int32_t(rows));
    h.i32(2, 0);
    h.i32(3, 3);
    h.i32(4, 3);
    h.end();
    h.b += char(0);
    int64_t off = int64_t(out.size());
    out += h.b;
    out += levels;
    out += c->data;
    int64_t size = int64_t(out.size()) - off;
    chunks.push_back({off, size});
    total += size;
  }
  Thrift m;
  m.i32(1, 1);
  m.list(2, 12, cols.size() + 1);
  m.begin();
  m.str(4, "schema");
  m.i32(5, int32_t(cols.size()));
  m.end();
  for (Column* c : cols) {
    m.begin();
    m.i32(1, c->type);
    m.i32(3, c->optional ? 1 : 0);
    m.str(4, c->name);
    if (c->logical == STRING) m.i32(6, 0);
    if (c->logical == DATE) m.i32(6, 6);
    if (c->logical != NOLOGICAL) {
      m.begin_struct(10);
      m.begin_struct(c->logical == STRING ? 1 : 6);
      m.end();
      m.end();
    }
    m.end();
  }
  m.i64(3, int64_t(rows));
  m.list(4, 12, rows ? 1 : 0);
  if (rows) {
    m.begin();
    m.list(1, 12, cols.size());
    for (size_t i = 0; i < cols.size(); ++i) {
      Column* c = cols[i];
      m.begin();
      m.i64(2, chunks[i].first);
      m.begin_struct(3);
      m.i32(1, c->type);
      m.list(2, 5, 2);
      m.zz(0);
      m.zz(3);
      m.list(3, 8, 1);
      m.bin(c->name);
      m.i32(4, 0);
      m.i64(5, int64_t(rows));
      m.i64(6, chunks[i].second);
      m.i64(7, chunks[i].second);
      m.i64(9, chunks[i].first);
      m.end();
      m.end();
    }
    m.i64(2, total);
    m.i64(3, int64_t(rows));
    m.end();
  }
  m.str(6, "shopping-sessions kernel");
  m.b += char(0);
  out += m.b;
  put_le32(out, uint32_t(m.b.size()));
  out += "PAR1";
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) die("create", path);
  for (size_t done = 0; done < out.size();) {
    ssize_t n = write(fd, out.data() + done, out.size() - done);
    if (n <= 0) die("write", path);
    done += size_t(n);
  }
  close(fd);
}

// ---------------------------------------------------------------- per worker

struct Cell {
  int64_t sessions, purchasing, revenue, assisted;
};

struct Worker {
  // campaign x day counters
  std::vector<Cell> cells;
  // assist window
  std::vector<uint32_t> count, first, pos, active;
  uint32_t ordinal = 0;
  int64_t wday = 0;
  // user-day category sums
  std::vector<int64_t> catsum;
  std::vector<uint32_t> touched;
  // outputs
  Column u_user{"user_id", BYTE_ARRAY, STRING, false, {}, {}, 0};
  Column u_day{"day", INT32, DATE, false, {}, {}, 0};
  Column u_sessions{"sessions", INT64, NOLOGICAL, false, {}, {}, 0};
  Column u_events{"events", INT64, NOLOGICAL, false, {}, {}, 0};
  Column u_revenue{"revenue_cents", INT64, NOLOGICAL, false, {}, {}, 0};
  Column u_top{"top_category", BYTE_ARRAY, STRING, true, {}, {}, 0};
  size_t rows = 0;
  double tm[5] = {0, 0, 0, 0, 0};
  // scratch
  std::vector<uint32_t> li, cnt, persons, cli, ccnt;
  std::vector<SE> stage, sorted;
  std::vector<SC> cstage, csorted;
  std::vector<KI> ki, kitmp;
};

struct Global {
  Time ws, we;
  int64_t day0;
  size_t ndays, ncamp;  // ncamp includes organic at 0
  std::vector<Text> camp_names, camp_channels;
  std::vector<Text> cat_names;
  std::vector<uint32_t> cat_rank;
};

static inline int64_t floordiv(Time a, Time b) {
  return a / b - (a % b < 0);
}

static inline Cell& cell(Worker& W, const Global& G, int64_t day,
                         uint32_t camp) {
  return W.cells[size_t(day - G.day0) * G.ncamp + camp];
}

static inline void charge(Worker& W, const Global& G, uint32_t c) {
  uint32_t& f = W.first[c];
  if (W.ordinal > f) cell(W, G, W.wday, c).assisted += W.ordinal - f;
  f = W.ordinal;
}

static void sweep(Worker& W, const Global& G, Text name, const SE* e,
                  size_t n, const SC* c, size_t m) {
  W.ordinal = 0;
  W.wday = INT64_MIN;
  int64_t uday = INT64_MIN, usess = 0, uev = 0, urev = 0;
  auto flush_user = [&]() {
    if (uday == INT64_MIN) return;
    W.u_user.put_str(name);
    W.u_day.put_i32(int32_t(uday));
    W.u_sessions.put_i64(usess);
    W.u_events.put_i64(uev);
    W.u_revenue.put_i64(urev);
    uint32_t best = NONE;
    for (uint32_t k : W.touched) {
      if (best == NONE || W.catsum[k] > W.catsum[best] ||
          (W.catsum[k] == W.catsum[best] && G.cat_rank[k] < G.cat_rank[best]))
        best = k;
    }
    W.u_top.present(best != NONE);
    if (best != NONE) W.u_top.put_str(G.cat_names[best]);
    for (uint32_t k : W.touched) W.catsum[k] = 0;
    W.touched.clear();
    ++W.rows;
  };
  size_t i = 0, L = 0, R = 0;
  while (i < n) {
    Time S = e[i].ts, low = S - WEEK;
    int64_t day = floordiv(S, DAY);
    if (day != W.wday) {
      if (W.wday != INT64_MIN)
        for (uint32_t a : W.active) charge(W, G, a);
      W.wday = day;
    }
    while (L < R && c[L].ts < low) {
      uint32_t a = c[L++].camp;
      if (--W.count[a] == 0) {
        charge(W, G, a);
        uint32_t back = W.active.back();
        W.active[W.pos[a]] = back;
        W.pos[back] = W.pos[a];
        W.active.pop_back();
      }
    }
    if (L == R) {
      while (R < m && c[R].ts < low) ++R;
      L = R;
    }
    while (R < m && c[R].ts <= S) {
      uint32_t a = c[R++].camp;
      if (W.count[a]++ == 0) {
        W.first[a] = W.ordinal;
        W.pos[a] = uint32_t(W.active.size());
        W.active.push_back(a);
      }
    }
    uint32_t credit = 0;
    if (L < R) {
      size_t b = R - 1;
      Time bt = c[b].ts;
      for (size_t k = b; k > L && c[k - 1].ts == bt;) {
        --k;
        if (Text(c[k].id, c[k].idlen) < Text(c[b].id, c[b].idlen)) b = k;
      }
      credit = c[b].camp;
    }
    if (day != uday) {
      flush_user();
      uday = day;
      usess = uev = urev = 0;
    }
    size_t j = i;
    int64_t rev = 0;
    do {
      if (uint32_t p = e[j].price) {
        rev += p;
        uint32_t k = e[j].cat;
        if (W.catsum[k] == 0) W.touched.push_back(k);
        W.catsum[k] += p;
      }
      ++j;
    } while (j < n && e[j].ts - e[j - 1].ts <= GAP);
    Cell& cc = cell(W, G, day, credit);
    cc.sessions += 1;
    cc.purchasing += rev > 0;
    cc.revenue += rev;
    usess += 1;
    uev += int64_t(j - i);
    urev += rev;
    ++W.ordinal;
    i = j;
  }
  if (W.wday != INT64_MIN)
    for (uint32_t a : W.active) charge(W, G, a);
  for (uint32_t a : W.active) W.count[a] = 0;
  W.active.clear();
  flush_user();
}

// ---------------------------------------------------------------- main run

static int ncpu() {
  if (const char* s = getenv("NCPU")) {
    int n = atoi(s);
    if (n > 0) return n;
  }
  cpu_set_t set;
  if (sched_getaffinity(0, sizeof(set), &set) == 0) return CPU_COUNT(&set);
  return int(std::thread::hardware_concurrency());
}

static std::string env(const char* k, const char* dflt) {
  const char* v = getenv(k);
  return v && *v ? v : dflt;
}

static Time manifest_time(const std::string& text, const char* key) {
  size_t k = text.find(std::string("\"") + key + "\"");
  if (k == std::string::npos) die("manifest key", key);
  size_t colon = text.find(':', k);
  size_t a = text.find('"', colon + 1);
  size_t b = text.find('"', a + 1);
  if (colon == std::string::npos || a == std::string::npos ||
      b == std::string::npos)
    die("manifest value", key);
  Time t = timestamp(Text(text).substr(a + 1, b - a - 1));
  if (t == BAD) die("manifest timestamp", key);
  return t;
}

static size_t pow2_at_least(size_t v) {
  size_t p = 1;
  while (p < v) p *= 2;
  return p;
}
static int log2_of(size_t p) { return __builtin_ctzll(p); }

int main() {
  t_start = now();
  {
    void* a = mmap(nullptr, ARENA, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (a == MAP_FAILED) die("arena");
    madvise(a, ARENA, MADV_HUGEPAGE);
    arena_base = static_cast<char*>(a);
  }
  verbose = getenv("KERNEL_QUIET") == nullptr;
  init_cls();
  T = std::clamp(ncpu(), 1, 256);
  std::string data = env("DATA_DIR", "/data"), out = env("OUTPUT_DIR", "/output");

  std::string manifest;
  {
    size_t size;
    const char* m = map_file(data + "/manifest.json", size);
    if (m) manifest.assign(m, size);
  }
  Global G;
  G.ws = manifest_time(manifest, "window_start");
  G.we = manifest_time(manifest, "window_end");
  G.day0 = floordiv(G.ws, DAY);
  G.ndays = size_t(floordiv(G.we - 1, DAY) - G.day0 + 1);

  constexpr size_t CHUNK = 1 << 20;
  Input links, products, campaigns, events, clicks;
  {
    std::vector<std::pair<Input*, Mapped>> files;
    files.push_back({&products, {data + "/products.csv"}});
    files.push_back({&campaigns, {data + "/campaigns.csv"}});
    for (auto& f : csv_files(data + "/links")) files.push_back({&links, {f}});
    for (auto& f : csv_files(data + "/events")) files.push_back({&events, {f}});
    for (auto& f : csv_files(data + "/clicks")) files.push_back({&clicks, {f}});
    // Large files first so the parallel populate finishes evenly.
    std::vector<size_t> order(files.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::vector<size_t> sizes(files.size());
    for (size_t i = 0; i < files.size(); ++i) {
      struct stat st;
      sizes[i] = stat(files[i].second.path.c_str(), &st) ? 0 : size_t(st.st_size);
    }
    std::sort(order.begin(), order.end(),
              [&](size_t a, size_t b) { return sizes[a] > sizes[b]; });
    parallel(files.size(), [&](int, size_t k) {
      Mapped& mf = files[order[k]].second;
      mf.data = map_file(mf.path, mf.size);
    });
    // Read-ahead hints run in the background, links first, so parsing starts
    // at once and page-cache misses overlap with work.
    if (!getenv("KERNEL_POPULATE")) {
      std::vector<std::pair<const char*, size_t>> hint;
      for (int pass = 0; pass < 2; ++pass)
        for (auto& [in, mf] : files)
          if (mf.data && ((in == &links) == (pass == 0)))
            hint.push_back({mf.data, mf.size});
      std::thread([hint] {
        for (auto [d, n] : hint)
          madvise(const_cast<char*>(d), n, MADV_WILLNEED);
      }).detach();
    }
    for (auto& [in, mf] : files)
      add_mapped(*in, mf, in == &products || in == &campaigns ? SIZE_MAX : CHUNK);
  }
  mark("mapped");

  // Products and categories.
  StrMap product;
  {
    std::vector<std::pair<Text, uint32_t>> items;
    std::vector<std::pair<Text, uint32_t>> cats;
    StrMap catmap;
    std::vector<Text> names;
    std::vector<std::pair<Text, Text>> rows;
    for (auto& c : products.chunks)
      each_row<2>(c, [&](Text* f) { rows.push_back({f[0], f[1]}); });
    // distinct categories
    std::vector<Text> distinct;
    for (auto& r : rows) distinct.push_back(r.second);
    distinct.push_back("unknown");
    std::sort(distinct.begin(), distinct.end());
    distinct.erase(std::unique(distinct.begin(), distinct.end()),
                   distinct.end());
    G.cat_names = distinct;  // sorted: index == byte-wise rank
    G.cat_rank.resize(distinct.size());
    for (uint32_t i = 0; i < distinct.size(); ++i) {
      G.cat_rank[i] = i;
      cats.push_back({distinct[i], i});
    }
    catmap.build(cats);
    for (auto& r : rows) items.push_back({r.first, catmap.find(r.second)});
    product.build(items);
    uint32_t unknown = catmap.find("unknown");
    // Encode "not found" through a sentinel slot lookup below.
    G.cat_rank.push_back(unknown);  // stash at the end: index of unknown
  }
  const uint32_t UNKNOWN_CAT = G.cat_rank.back();
  G.cat_rank.pop_back();
  if (G.cat_names.size() > 65535) die("too many categories");

  // Campaigns.
  StrMap campaign;
  {
    std::vector<std::pair<Text, uint32_t>> items;
    G.camp_names.push_back("organic");
    G.camp_channels.push_back("organic");
    for (auto& c : campaigns.chunks)
      each_row<2>(c, [&](Text* f) {
        items.push_back({f[0], uint32_t(G.camp_names.size())});
        G.camp_names.push_back(f[0]);
        G.camp_channels.push_back(f[1]);
      });
    campaign.build(items);
    G.ncamp = G.camp_names.size();
  }

  // Sizing.
  const size_t P = getenv("KP") ? size_t(atoi(getenv("KP")))
                                : std::clamp<size_t>(pow2_at_least(events.bytes / (4 << 20)), 64, 2048);
  const int logP = log2_of(P);
  const size_t est_event_lines = events.bytes / 60 + 1024;
  const size_t BITS = std::clamp<size_t>(pow2_at_least(est_event_lines * 32),
                                         size_t(1) << 22, size_t(1) << 32);
  const size_t D = std::max(getenv("KD") ? size_t(atoi(getenv("KD"))) : P,
                            BITS >> 23);  // local bitmap index fits 23 bits
  if (events.chunks.size() >= (size_t(1) << 21)) die("too many event chunks");
  for (auto& c : events.chunks)
    if (size_t(c.end - c.begin) > (size_t(1) << 20)) die("event chunk over 1 MB");
  const int logBITS = log2_of(BITS);
  const int logLocal = logBITS - log2_of(D);
  const size_t local_bits = size_t(1) << logLocal;
  auto ppart = [&](uint32_t person) {
    return size_t((person * 2654435761u) >> (32 - logP));
  };

  size_t est_ids = links.bytes / 12 + (events.bytes + clicks.bytes) / 1000 +
                   size_t(T) * IdTable::BLOCK * 2 + 4096;
  IdTable ids;
  std::vector<AVec<Ev>> ev;
  std::vector<AVec<Dd>> dd;
  std::vector<AVec<Ck>> ck;
  for (int attempt = 0;; ++attempt) {
    ids.~IdTable();
    new (&ids) IdTable();
    ids.init(pow2_at_least(est_ids) << (2 * attempt));

    // ---- links
    {
      std::vector<std::vector<std::pair<uint32_t, uint32_t>>> edges(T);
      parallel(links.chunks.size(), [&](int w, size_t i) {
        each_row<2>(links.chunks[i], [&](Text* f) {
          if (f[0].empty() || f[1].empty()) return;
          uint32_t a = ids.get(f[0], hash_text(f[0]), w)->id;
          uint32_t b = ids.get(f[1], hash_text(f[1]), w)->id;
          edges[w].push_back({a, b});
        });
      });
      if (ids.overflow) continue;
      uint32_t n = std::min(ids.next.load(), ids.cap);
      mark("links-parsed");
      std::vector<uint32_t> parent(n);
      for (uint32_t i = 0; i < n; ++i) parent[i] = i;
      auto find = [&](uint32_t x) {
        while (parent[x] != x) {
          parent[x] = parent[parent[x]];
          x = parent[x];
        }
        return x;
      };
      for (auto& list : edges)
        for (auto [a, b] : list) {
          uint32_t x = find(a), y = find(b);
          if (x != y) {
            if (x < y) std::swap(x, y);
            parent[x] = y;
          }
        }
      mark("links-union");
      // Full path compression in parallel (read-only walks), then a parallel
      // byte-wise minimum per component with CAS.
      std::vector<uint32_t> root(n);
      parallel((n + 65535) / 65536, [&](int, size_t b) {
        uint32_t lo = uint32_t(b * 65536), hi = std::min<uint32_t>(n, lo + 65536);
        for (uint32_t i = lo; i < hi; ++i) {
          uint32_t x = i;
          while (parent[x] != x) x = parent[x];
          root[i] = x;
        }
      });
      std::vector<uint32_t> best(root);  // a component's root starts as its best
      parallel((n + 65535) / 65536, [&](int, size_t b) {
        uint32_t lo = uint32_t(b * 65536), hi = std::min<uint32_t>(n, lo + 65536);
        for (uint32_t i = lo; i < hi; ++i) {
          uint32_t r = root[i];
          if (r == i) continue;
          uint32_t cur = __atomic_load_n(&best[r], __ATOMIC_RELAXED);
          while (ids.strs[i] < ids.strs[cur] &&
                 !__atomic_compare_exchange_n(&best[r], &cur, i, false,
                                              __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
          }
        }
      });
      parallel((n + 65535) / 65536, [&](int, size_t b) {
        uint32_t lo = uint32_t(b * 65536), hi = std::min<uint32_t>(n, lo + 65536);
        for (uint32_t i = lo; i < hi; ++i) {
          uint32_t r = root[i];
          if (r == i && best[r] == i) continue;  // singleton or own best
          ids.slots[ids.slot_of[i]].person = best[r];
        }
      });
    }
    mark("links");

    // ---- events and clicks
    ev.assign(size_t(T) * P, {});
    dd.assign(size_t(T) * D, {});
    ck.assign(size_t(T) * P, {});
    {
      size_t per_ev = est_event_lines * 5 / 4 / (size_t(T) * P) + 64;
      size_t per_ck = clicks.bytes / 50 / (size_t(T) * P) + 16;
      parallel(size_t(T), [&](int, size_t t) {
        for (size_t p = 0; p < P; ++p) {
          ev[t * P + p].reserve(per_ev);
          ck[t * P + p].reserve(per_ck);
        }
        size_t per_dd = est_event_lines * 5 / 4 / (size_t(T) * D) + 64;
        for (size_t d = 0; d < D; ++d) dd[t * D + d].reserve(per_dd);
      });
    }
    const size_t nev = events.chunks.size();
    const Time ws = G.ws, we = G.we, cws = G.ws - WEEK;
    parallel(nev + clicks.chunks.size(), [&](int w, size_t job) {
      Text f[7];
      bool ok;
      // Rows wait in a small batch so that their ID-slot misses overlap.
      constexpr int BATCH = 16;
      struct PE { Text user; uint64_t h; Ev e; } pend[BATCH];
      struct PC { Text user; uint64_t h; Ck c; } cpend[BATCH];
      int np = 0;
      auto flush_ev = [&]() {
        for (int k = 0; k < np; ++k) {
          Ev e = pend[k].e;
          e.person = ids.get(pend[k].user, pend[k].h, w)->person;
          ev[size_t(w) * P + ppart(e.person)].push_back(e);
        }
        np = 0;
      };
      auto flush_ck = [&]() {
        for (int k = 0; k < np; ++k) {
          Ck c = cpend[k].c;
          c.person = ids.get(cpend[k].user, cpend[k].h, w)->person;
          ck[size_t(w) * P + ppart(c.person)].push_back(c);
        }
        np = 0;
      };
      if (job < nev) {
        const Chunk& c = events.chunks[job];
        for (const char* p = first_line(c); p < c.end;) {
          const char* line = p;
          p = split<7>(p, f, ok);
          if (!ok || f[0].empty()) continue;
          uint64_t h = hash_text(f[0]);
          uint32_t g = uint32_t(h & (BITS - 1));
          dd[size_t(w) * D + (g >> logLocal)].push_back(
              {(uint64_t(g & (local_bits - 1)) << 41) |
               (uint64_t(line - c.begin) << 21) | job});
          if (f[1].empty() || f[5].empty()) continue;
          uint32_t price = 0;
          bool purchase = false;
          Text ty = f[4];
          if (ty.size() == 4) {
            if (ty != "view" && ty != "cart") continue;
          } else if (ty == "purchase") {
            Time pr = decimal(f[6], 10000000);
            if (pr < 1) continue;
            price = uint32_t(pr);
            purchase = true;
          } else {
            continue;
          }
          Time ts = timestamp(f[2]);
          if (ts < ws || ts >= we) continue;
          Time ingest = timestamp(f[3]);
          if (ingest - ts > DAY) continue;
          uint32_t cat = 0;
          if (purchase) {
            cat = product.find(f[5]);
            if (cat == NONE) cat = UNKNOWN_CAT;
          }
          uint64_t uh = hash_text(f[1]);
          __builtin_prefetch(ids.slots + (uh & ids.mask));
          pend[np++] = {f[1], uh, {ts, 0, price, g, uint16_t(cat), 0}};
          if (np == BATCH) flush_ev();
        }
        flush_ev();
      } else {
        const Chunk& c = clicks.chunks[job - nev];
        for (const char* p = first_line(c); p < c.end;) {
          p = split<4>(p, f, ok);
          if (!ok || f[0].empty() || f[1].empty()) continue;
          uint32_t camp = campaign.find(f[2]);
          if (camp == NONE) continue;
          Time ts = timestamp(f[3]);
          if (ts < cws || ts >= we) continue;
          uint64_t uh = hash_text(f[1]);
          __builtin_prefetch(ids.slots + (uh & ids.mask));
          cpend[np++] = {f[1], uh, {ts, 0, camp, f[0].data(), uint32_t(f[0].size()), 0}};
          if (np == BATCH) flush_ck();
        }
        flush_ck();
      }
    });
    if (ids.overflow) continue;
    break;
  }
  mark("parse");

  // ---- dedup
  uint64_t* rep = static_cast<uint64_t*>(alloc_zero(BITS / 8));
  std::vector<AVec<Ev>> winners(size_t(T) * P);
  {
    const size_t wpp = local_bits / 64;
    std::vector<std::vector<uint64_t>> seen(T);
    struct Best {
      uint64_t h;
      Text key;
      Time ingest;
      const char* line;
    };
    std::vector<std::vector<Best>> table(T);
    std::vector<std::vector<const char*>> contested(T);
    parallel(D, [&](int w, size_t d) {
      auto& s = seen[w];
      s.assign(wpp, 0);
      uint64_t* r = rep + d * wpp;
      for (int t = 0; t < T; ++t)
        for (const Dd& x : dd[size_t(t) * D + d]) {
          uint32_t lc = x.local();
          uint64_t bit = uint64_t(1) << (lc & 63);
          uint64_t& sw = s[lc >> 6];
          if (sw & bit)
            r[lc >> 6] |= bit;
          else
            sw |= bit;
        }
      auto& list = contested[w];
      list.clear();
      for (int t = 0; t < T; ++t)
        for (const Dd& x : dd[size_t(t) * D + d])
          if ((r[x.local() >> 6] >> (x.local() & 63)) & 1)
            list.push_back(events.chunks[x.chunk()].begin + x.offset());
      if (list.empty()) return;
      auto& tb = table[w];
      size_t cap = pow2_at_least(list.size() * 2 + 16);
      tb.assign(cap, Best{0, Text(), 0, nullptr});
      Text f[7];
      bool ok;
      for (size_t li2 = 0; li2 < list.size(); ++li2) {
        if (li2 + 8 < list.size()) {
          __builtin_prefetch(list[li2 + 8]);
          __builtin_prefetch(list[li2 + 8] + 64);
        }
        const char* line = list[li2];
        split<7>(line, f, ok);
        uint64_t h = hash_text(f[0]);
        Time ingest = timestamp(f[3]);
        size_t i = h & (cap - 1);
        for (;;) {
          Best& b = tb[i];
          if (!b.line) {
            b = {h, f[0], ingest, line};
            break;
          }
          if (b.h == h && b.key == f[0]) {
            if (ingest < b.ingest) {
              b.ingest = ingest;
              b.line = line;
              b.key = f[0];
            }
            break;
          }
          i = (i + 1) & (cap - 1);
        }
      }
      for (const Best& b : tb) {
        if (!b.line) continue;
        split<7>(b.line, f, ok);
        if (f[1].empty() || f[5].empty()) continue;
        uint32_t price = 0;
        bool purchase = false;
        Text ty = f[4];
        if (ty.size() == 4) {
          if (ty != "view" && ty != "cart") continue;
        } else if (ty == "purchase") {
          Time pr = decimal(f[6], 10000000);
          if (pr < 1) continue;
          price = uint32_t(pr);
          purchase = true;
        } else {
          continue;
        }
        Time ts = timestamp(f[2]);
        if (ts < G.ws || ts >= G.we) continue;
        if (b.ingest - ts > DAY) continue;
        uint32_t cat = 0;
        if (purchase) {
          cat = product.find(f[5]);
          if (cat == NONE) cat = UNKNOWN_CAT;
        }
        uint32_t person = ids.get(f[1], hash_text(f[1]), w)->person;
        winners[size_t(w) * P + ppart(person)].push_back(
            {ts, person, price, 0, uint16_t(cat), 0});
      }
    });
    std::vector<AVec<Dd>>().swap(dd);
  }
  mark("dedup");

  // ---- sessions
  const uint32_t nids = std::min(ids.next.load(), ids.cap);
  uint32_t* local_of = static_cast<uint32_t*>(alloc_zero(size_t(nids) * 4 + 4));
  memset(local_of, 0xFF, size_t(nids) * 4);
  std::vector<Worker> workers(T);
  for (auto& W : workers) {
    W.cells.assign(G.ndays * G.ncamp, Cell{0, 0, 0, 0});
    W.count.assign(G.ncamp, 0);
    W.first.assign(G.ncamp, 0);
    W.pos.assign(G.ncamp, 0);
    W.catsum.assign(G.cat_names.size(), 0);
    size_t rows_est = events.bytes / 80 / 9 / size_t(T) + 1024;
    W.u_user.data.reserve(rows_est * 16);
    W.u_day.data.reserve(rows_est * 4);
    W.u_sessions.data.reserve(rows_est * 8);
    W.u_events.data.reserve(rows_est * 8);
    W.u_revenue.data.reserve(rows_est * 8);
    W.u_top.data.reserve(rows_est * 4);
    W.u_top.def.reserve(rows_est / 8 + 8);
  }
  std::vector<std::pair<size_t, size_t>> order(P);
  for (size_t p = 0; p < P; ++p) {
    size_t s = 0;
    for (int t = 0; t < T; ++t) s += ev[size_t(t) * P + p].size();
    order[p] = {s, p};
  }
  std::sort(order.rbegin(), order.rend());
  // Relative times fit 32 bits whenever the click window spans < ~49 days.
  const bool fast_sort = (G.we - (G.ws - WEEK)) < (Time(1) << 32);
  mark("sessions-setup");
  parallel(P, [&](int w, size_t job) {
    size_t p = order[job].second;
    double tjob = now();
    Worker& W = workers[w];
    auto& li = W.li;
    auto& cnt = W.cnt;
    auto& persons = W.persons;
    auto& stage = W.stage;
    li.clear();
    cnt.clear();
    persons.clear();
    stage.clear();
    auto take = [&](const Ev& e) {
      uint32_t l = local_of[e.person];
      if (l == NONE) {
        l = local_of[e.person] = uint32_t(persons.size());
        persons.push_back(e.person);
        cnt.push_back(0);
      }
      ++cnt[l];
      li.push_back(l);
      stage.push_back({e.ts, e.price, e.cat});
    };
    for (int t = 0; t < T; ++t) {
      const auto& v = ev[size_t(t) * P + p];
      const size_t nv = v.size();
      for (size_t k = 0; k < nv; ++k) {
        if (k + 16 < nv) {
          __builtin_prefetch(rep + (v[k + 16].slot >> 6));
          __builtin_prefetch(local_of + v[k + 16].person);
        }
        const Ev& e = v[k];
        if (!((rep[e.slot >> 6] >> (e.slot & 63)) & 1)) take(e);
      }
      AVec<Ev>().swap(ev[size_t(t) * P + p]);
      for (const Ev& e : winners[size_t(t) * P + p]) take(e);
    }
    double tg = now();
    size_t np = persons.size();
    if (!np) return;
    // counting sort events by local person
    std::vector<uint32_t> start(np + 1);
    for (size_t k = 0; k < np; ++k) start[k + 1] = start[k] + cnt[k];
    auto& sorted = W.sorted;
    sorted.resize(stage.size());
    {
      std::vector<uint32_t> at(start.begin(), start.end() - 1);
      if (fast_sort) {
        auto& ki = W.ki;
        ki.resize(stage.size());
        for (size_t k = 0; k < stage.size(); ++k)
          ki[k] = {uint32_t(stage[k].ts - G.ws), uint32_t(k)};
        radix32(ki, W.kitmp);
        for (const KI& x : ki) sorted[at[li[x.idx]]++] = stage[x.idx];
      } else {
        for (size_t k = 0; k < stage.size(); ++k) sorted[at[li[k]]++] = stage[k];
      }
    }
    // clicks
    double ts1 = now();
    auto& cli = W.cli;
    auto& ccnt = W.ccnt;
    auto& cstage = W.cstage;
    cli.clear();
    cstage.clear();
    ccnt.assign(np, 0);
    for (int t = 0; t < T; ++t) {
      const auto& cv = ck[size_t(t) * P + p];
      for (size_t k = 0; k < cv.size(); ++k) {
        if (k + 16 < cv.size()) __builtin_prefetch(local_of + cv[k + 16].person);
        const Ck& c = cv[k];
        uint32_t l = local_of[c.person];
        if (l == NONE) continue;
        ++ccnt[l];
        cli.push_back(l);
        cstage.push_back({c.ts, c.camp, c.idlen, c.id});
      }
      AVec<Ck>().swap(ck[size_t(t) * P + p]);
    }
    std::vector<uint32_t> cstart(np + 1);
    for (size_t k = 0; k < np; ++k) cstart[k + 1] = cstart[k] + ccnt[k];
    auto& csorted = W.csorted;
    csorted.resize(cstage.size());
    {
      std::vector<uint32_t> at(cstart.begin(), cstart.end() - 1);
      if (fast_sort) {
        auto& ki = W.ki;
        ki.resize(cstage.size());
        for (size_t k = 0; k < cstage.size(); ++k)
          ki[k] = {uint32_t(cstage[k].ts - (G.ws - WEEK)), uint32_t(k)};
        radix32(ki, W.kitmp);
        for (const KI& x : ki) csorted[at[cli[x.idx]]++] = cstage[x.idx];
      } else {
        for (size_t k = 0; k < cstage.size(); ++k)
          csorted[at[cli[k]]++] = cstage[k];
      }
    }
    double ts2 = now();
    for (size_t k = 0; k < np; ++k) {
      SE* e = sorted.data() + start[k];
      size_t n = start[k + 1] - start[k];
      SC* c = csorted.data() + cstart[k];
      size_t m = cstart[k + 1] - cstart[k];
      if (!fast_sort) {
        std::sort(e, e + n, [](const SE& a, const SE& b) { return a.ts < b.ts; });
        std::sort(c, c + m, [](const SC& a, const SC& b) { return a.ts < b.ts; });
      }
      sweep(W, G, ids.strs[persons[k]], e, n, c, m);
    }
    double ts3 = now();
    W.tm[0] += tg - tjob; W.tm[1] += ts1 - tg; W.tm[2] += ts2 - ts1; W.tm[3] += ts3 - ts2;
  });
  mark("sessions");
  if (verbose) {
    double tm[4] = {0, 0, 0, 0};
    for (auto& W : workers) for (int i = 0; i < 4; ++i) tm[i] += W.tm[i];
    double sw = 0; for (auto& W : workers) sw += W.tm[4];
    fprintf(stderr, "  cpu: gather %.3f  ev-sort %.3f  clicks %.3f  sort+sweep %.3f  sweep %.3f\n", tm[0], tm[1], tm[2], tm[3], sw);
  }

  // ---- outputs
  std::string ud = out + "/user_daily", cd = out + "/campaign_daily";
  mkdir(out.c_str(), 0777);
  mkdir(ud.c_str(), 0777);
  mkdir(cd.c_str(), 0777);
  size_t total_rows = 0;
  for (auto& W : workers) total_rows += W.rows;
  parallel(size_t(T) + 1, [&](int, size_t t) {
    if (t == size_t(T)) {
      Column c_id{"campaign_id", BYTE_ARRAY, STRING, false, {}, {}, 0};
      Column c_ch{"channel", BYTE_ARRAY, STRING, false, {}, {}, 0};
      Column c_day{"day", INT32, DATE, false, {}, {}, 0};
      Column c_s{"sessions", INT64, NOLOGICAL, false, {}, {}, 0};
      Column c_p{"purchasing_sessions", INT64, NOLOGICAL, false, {}, {}, 0};
      Column c_r{"revenue_cents", INT64, NOLOGICAL, false, {}, {}, 0};
      Column c_a{"assisted_sessions", INT64, NOLOGICAL, false, {}, {}, 0};
      size_t rows = 0;
      for (size_t d = 0; d < G.ndays; ++d)
        for (size_t c = 0; c < G.ncamp; ++c) {
          Cell sum{0, 0, 0, 0};
          for (auto& W : workers) {
            const Cell& x = W.cells[d * G.ncamp + c];
            sum.sessions += x.sessions;
            sum.purchasing += x.purchasing;
            sum.revenue += x.revenue;
            sum.assisted += x.assisted;
          }
          if (!sum.sessions && !sum.assisted) continue;
          c_id.put_str(G.camp_names[c]);
          c_ch.put_str(G.camp_channels[c]);
          c_day.put_i32(int32_t(G.day0 + int64_t(d)));
          c_s.put_i64(sum.sessions);
          c_p.put_i64(sum.purchasing);
          c_r.put_i64(sum.revenue);
          c_a.put_i64(c == 0 ? 0 : sum.assisted);
          ++rows;
        }
      write_parquet(cd + "/part-0.parquet", rows,
                    {&c_id, &c_ch, &c_day, &c_s, &c_p, &c_r, &c_a});
      return;
    }
    Worker& W = workers[t];
    if (!W.rows && !(t == 0 && total_rows == 0)) return;
    write_parquet(ud + "/part-" + std::to_string(t) + ".parquet", W.rows,
                  {&W.u_user, &W.u_day, &W.u_sessions, &W.u_events,
                   &W.u_revenue, &W.u_top});
  });
  mark("written");
  fflush(stderr);
  if (getenv("KERNEL_SLOW_EXIT")) exit(0);
  _exit(0);
}
