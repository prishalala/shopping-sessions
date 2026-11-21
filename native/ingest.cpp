// Parallel LF-delimited ingestion. Input-backed views remain alive through
// dedup.
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "parse.hpp"

namespace fs = std::filesystem;
constexpr size_t buckets = 256;
size_t workers;

struct Mapping {
  const char* data = nullptr;
  size_t size;
  explicit Mapping(const fs::path& path) : size(fs::file_size(path)) {
    if (!size) return;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("open: " + path.string());
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) throw std::runtime_error("mmap: " + path.string());
    data = static_cast<const char*>(ptr);
  }
  ~Mapping() {
    if (data) munmap(const_cast<char*>(data), size);
  }
};

struct Inputs {
  std::vector<std::unique_ptr<Mapping>> files;
  std::vector<Text> chunks;
  size_t bytes = 0;
  void add(const fs::path& path) {
    auto& file = *files.emplace_back(std::make_unique<Mapping>(path));
    bytes += file.size;
    if (!file.size) return;
    Text tail(file.data, file.size);
    size_t header = tail.find('\n');
    if (header == Text::npos) return;
    tail.remove_prefix(header + 1);
    while (!tail.empty()) {
      size_t end = tail.find('\n', 2 << 20);
      size_t len = end == Text::npos ? tail.size() : end + 1;
      chunks.push_back(tail.substr(0, len));
      tail.remove_prefix(len);
    }
  }
  void folder(const fs::path& path) {
    if (!fs::exists(path)) return;
    for (auto& entry : fs::directory_iterator(path))
      if (entry.path().extension() == ".csv" && entry.is_regular_file())
        add(entry.path());
  }
};

template <size_t N, class Fn>
void each_row(Text chunk, Fn fn) {
  std::array<Text, N> row;
  while (!chunk.empty()) {
    size_t end = chunk.find('\n');
    Text line = chunk.substr(0, end);
    if (fields(line, row)) fn(row);
    chunk.remove_prefix(end == Text::npos ? chunk.size() : end + 1);
  }
}

template <class Fn>
void parallel(size_t jobs, Fn fn) {
  std::atomic<size_t> next{0};
  std::vector<std::thread> pool;
  for (size_t w = 0; w < std::min(jobs, workers); ++w)
    pool.emplace_back([&, w] {
      for (size_t i; (i = next.fetch_add(1, std::memory_order_relaxed)) < jobs;)
        fn(w, i);
    });
  for (auto& t : pool) t.join();
}

struct People {
  std::unordered_map<Text, uint32_t> index;
  std::vector<uint32_t> parent, weight;
  std::vector<Text> names;

  uint32_t id(Text s) {
    auto [it, fresh] = index.try_emplace(s, parent.size());
    if (fresh) {
      parent.push_back(it->second);
      weight.push_back(1);
      names.push_back(s);
    }
    return it->second;
  }
  uint32_t root(uint32_t i) {
    while (i != parent[i]) {
      parent[i] = parent[parent[i]];
      i = parent[i];
    }
    return i;
  }
  void connect(Text a, Text b) {
    if (a.empty() || b.empty()) return;
    auto x = root(id(a)), y = root(id(b));
    if (x == y) return;
    if (weight[x] < weight[y]) std::swap(x, y);
    parent[y] = x;
    weight[x] += weight[y];
    names[x] = std::min(names[x], names[y]);
  }
  void finish() {
    for (auto& [key, i] : index) i = root(i);
  }
  Text name(Text s) const {
    auto it = index.find(s);
    return it == index.end() ? s : names[it->second];
  }
};

void append_number(std::string& out, Time v) {
  char bytes[24];
  auto [end, err] = std::to_chars(bytes, bytes + sizeof(bytes), v);
  out.append(bytes, end);
}

#include "sessions.hpp"

struct Candidate {
  Text key, user, category;
  Time ingest, ts;
  uint32_t price;
};

// A clear bit in repeated proves that the ID appeared only once. Collisions
// only send additional rows to the exact byte-key table below.
class RepeatFilter {
  std::vector<uint64_t> repeated;

 public:
  explicit RepeatFilter(const Inputs& input) {
    size_t words = 1;
    while (words < input.bytes / 512) words *= 2;
    repeated.resize(words);
    std::vector<uint64_t> seen(words);
    parallel(input.chunks.size(), [&](size_t, size_t i) {
      Text chunk = input.chunks[i];
      while (!chunk.empty()) {
        auto lf = chunk.find('\n');
        Text line = chunk.substr(0, lf);
        chunk.remove_prefix(lf == Text::npos ? chunk.size() : lf + 1);
        auto comma = line.find(',');
        if (comma == Text::npos) continue;
        Text key = line.substr(0, comma);
        if (key.size() >= 2 && key.front() == '"' && key.back() == '"')
          key = key.substr(1, key.size() - 2);
        if (key.empty()) continue;
        size_t hash = std::hash<Text>{}(key);
        size_t word = (hash >> 6) & (words - 1);
        uint64_t bit = uint64_t{1} << (hash & 63);
        if (__atomic_fetch_or(&seen[word], bit, __ATOMIC_RELAXED) & bit)
          __atomic_fetch_or(&repeated[word], bit, __ATOMIC_RELAXED);
      }
    });
  }
  bool maybe_repeated(size_t hash) const {
    return (repeated[(hash >> 6) & (repeated.size() - 1)] >> (hash & 63)) & 1;
  }
};

void events(Inputs& files, const People& people,
            const std::unordered_map<Text, Text>& products, Time start,
            Time end, BufferedRows<RawHit>& output) {
  RepeatFilter filter(files);
  auto emit = [&](size_t w, const Candidate& row) {
    if (row.ts == bad_time) return;
    Text user = people.name(row.user);
    output.push(w, {row.ts, user, row.category, row.price});
  };
  using Bins = std::array<std::vector<Candidate>, buckets>;
  std::vector<Bins> bins(workers);
  std::deque<std::pmr::monotonic_buffer_resource> key_storage(workers);
  parallel(files.chunks.size(), [&](size_t w, size_t i) {
    each_row<7>(files.chunks[i], [&](const auto& a) {
      if (a[0].empty()) return;
      auto hash = std::hash<Text>{}(a[0]);
      Candidate event{a[0], a[1], {}, timestamp(a[3]), bad_time, 0};
      auto validate = [&]() {
        if (a[1].empty() || a[5].empty()) return;
        Time ts = timestamp(a[2]);
        if (ts < start || ts >= end || event.ingest - ts > day_ms) return;
        if (a[4] == "purchase") {
          Time price = decimal(a[6], 10000000);
          if (price < 1) return;
          event.price = uint32_t(price);
          auto it = products.find(a[5]);
          event.category = it == products.end() ? "unknown" : it->second;
        } else if (a[4] != "cart" && a[4] != "view")
          return;
        event.ts = ts;
      };
      validate();
      // Invalid copies in a contested slot can still be the earliest.
      if (filter.maybe_repeated(hash)) {
        // Contested IDs must outlive this pass. Copy just these bytes so
        // probing a bucket cannot fault random pages back in from the entire
        // input.
        Text user = event.ts == bad_time ? Text{} : event.user;
        char* p = static_cast<char*>(
            key_storage[w].allocate(event.key.size() + user.size(), 1));
        std::memcpy(p, event.key.data(), event.key.size());
        event.key = {p, event.key.size()};
        p += event.key.size();
        if (!user.empty()) std::memcpy(p, user.data(), user.size());
        event.user = {p, user.size()};
        bins[w][hash % buckets].push_back(event);
      } else {
        emit(w, event);
      }
    });
  });
  // Flush the last singleton rows before releasing their input-backed names.
  output.finish();
  files.chunks.clear();
  files.files.clear();
  parallel(buckets, [&](size_t w, size_t b) {
    size_t count = 0;
    for (const auto& group : bins) count += group[b].size();
    size_t capacity = 1;
    while (capacity < count + count / 2) capacity *= 2;
    std::vector<const Candidate*> slots(capacity);
    for (auto& group : bins) {
      for (const auto& row : group[b]) {
        size_t idx = (std::hash<Text>{}(row.key) / buckets) & (capacity - 1);
        while (slots[idx] && slots[idx]->key != row.key)
          idx = (idx + 1) & (capacity - 1);
        if (!slots[idx] || row.ingest < slots[idx]->ingest) slots[idx] = &row;
      }
    }
    for (const Candidate* row : slots)
      if (row) emit(w, *row);
    for (auto& group : bins) std::vector<Candidate>().swap(group[b]);
  });
  output.finish();
}

int main(int argc, char** argv) {
  if (argc != 6) return 2;
  fs::path data = argv[1], scratch = argv[2];
  Time start = std::stoll(argv[3]), end = std::stoll(argv[4]);
  workers = std::max(1, std::stoi(argv[5]));
  Inputs links, products, campaigns, clicks, event_files;
  links.folder(data / "links");
  products.add(data / "products.csv");
  campaigns.add(data / "campaigns.csv");
  event_files.folder(data / "events");
  clicks.folder(data / "clicks");
  People people;
  for (auto chunk : links.chunks)
    each_row<2>(chunk, [&](auto a) { people.connect(a[0], a[1]); });
  people.finish();
  std::unordered_map<Text, Text> product;
  std::unordered_map<Text, uint32_t> channel;
  std::vector<Text> campaign_names{"organic"};
  for (auto chunk : products.chunks)
    each_row<2>(chunk, [&](auto a) { product[a[0]] = a[1]; });
  for (auto chunk : campaigns.chunks)
    each_row<2>(chunk, [&](auto a) {
      channel[a[0]] = campaign_names.size();
      campaign_names.push_back(a[0]);
    });
  const size_t n = std::min(
      1 + (event_files.bytes + clicks.bytes) / (8ULL << 20), 4ULL * workers);
  std::deque<Bucket> partitions(n);
  BufferedRows<RawHit> event_out(partitions);
  events(event_files, people, product, start, end, event_out);
  BufferedRows<RawClick> click_out(partitions);
  parallel(clicks.chunks.size(), [&](size_t w, size_t i) {
    each_row<4>(clicks.chunks[i], [&](auto a) {
      auto campaign = channel.find(a[2]);
      if (a[0].empty() || a[1].empty() || campaign == channel.end()) return;
      Time ts = timestamp(a[3]);
      if (ts < start - 7 * day_ms || ts >= end) return;
      Text user = people.name(a[1]);
      click_out.push(w, {ts, user, a[0], campaign->second});
    });
  });
  click_out.finish();
  people = People{};
  parallel(n, [&](size_t, size_t i) {
    summarize(partitions[i], scratch, i, campaign_names);
  });
}
