// Each bucket owns complete people. Dense local user numbers keep the event
// sort independent of identifier length. The event array costs 16 bytes per
// record.
struct Hit {
  Time ts;
  uint32_t user, purchase;
};
struct AdClick {
  Time ts;
  uint32_t user, campaign;
  Text id;
};
struct Purchase {
  Text category;
  uint32_t price;
};
struct RawHit {
  Time ts;
  Text user, category;
  uint32_t price;
};
struct RawClick {
  Time ts;
  Text user, id;
  uint32_t campaign;
};

struct Bucket {
  std::mutex lock;
  std::unordered_map<Text, uint32_t> ids;
  std::deque<std::string> names;
  std::vector<Hit> events;
  std::vector<Purchase> purchases{{{}, 0}};
  std::vector<AdClick> clicks;

  uint32_t user_id(Text name) {
    auto it = ids.find(name);
    if (it != ids.end()) return it->second;
    uint32_t id = names.size();
    names.emplace_back(name);
    ids.emplace(names.back(), id);
    return id;
  }
  void add(const RawHit& row) {
    uint32_t purchase = 0;
    if (row.price) {
      purchase = purchases.size();
      purchases.push_back({row.category, row.price});
    }
    events.push_back({row.ts, user_id(row.user), purchase});
  }
  void add(const RawClick& row) {
    clicks.push_back({row.ts, user_id(row.user), row.campaign, row.id});
  }
};

template <class T>
class BufferedRows {
  std::deque<Bucket>& dest;
  std::vector<std::vector<std::vector<T>>> buffers;
  void flush(size_t w, size_t i) {
    auto& buf = buffers[w][i];
    if (buf.empty()) return;
    auto& bucket = dest[i];
    std::lock_guard guard(bucket.lock);
    for (const auto& row : buf) bucket.add(row);
    buf.clear();
  }

 public:
  explicit BufferedRows(std::deque<Bucket>& dest)
      : dest(dest),
        buffers(workers, std::vector<std::vector<T>>(dest.size())) {}
  void push(size_t w, const T& row) {
    size_t i = std::hash<Text>{}(row.user) % dest.size();
    auto& buf = buffers[w][i];
    buf.push_back(row);
    if (buf.size() >= 128) flush(w, i);
  }
  void finish() {
    for (size_t i = 0; i < dest.size(); ++i)
      for (size_t w = 0; w < workers; ++w) flush(w, i);
  }
};

// Buffered numeric CSV for Polars. Framing bytes protect the beginning and end
// of each text field (Polars' CSV reader strips terminal carriage returns).
class Sink {
  std::ofstream out;
  std::string buffer;

 public:
  explicit Sink(const fs::path& p) : out(p, std::ios::binary) {
    out.exceptions(std::ios::badbit | std::ios::failbit);
  }
  void number(Time v, char sep) {
    append_number(buffer, v);
    buffer += sep;
    if (sep == '\n' && buffer.size() >= 64 * 1024) flush();
  }
  void label(Text text, char sep) {
    buffer += '!';
    buffer.append(text);
    buffer += '!';
    buffer += sep;
    if (sep == '\n' && buffer.size() >= 64 * 1024) flush();
  }
  void flush() {
    out.write(buffer.data(), buffer.size());
    buffer.clear();
  }
  void close() {
    flush();
    out.close();
  }
};

struct Activity {
  uint32_t clicks = 0, first_session = 0, position = 0;
};

// Charge a run of consecutive sessions when the campaign's last click expires
// or the UTC day changes. Reuse the state array for every person in the bucket.
struct AssistWindow {
  explicit AssistWindow(size_t campaigns) : states(campaigns) {}
  std::vector<Activity> states;
  std::vector<uint32_t> active;
  std::unordered_map<int, std::unordered_map<uint32_t, int64_t>> totals;
  int day = 0;
  uint32_t ordinal = 0;

  void charge(uint32_t campaign) {
    auto& start = states[campaign].first_session;
    if (ordinal > start) totals[day][campaign] += ordinal - start;
    start = ordinal;
  }
  void close_day() {
    for (auto campaign : active) charge(campaign);
  }
  void finish_person() {
    close_day();
    for (auto campaign : active) states[campaign].clicks = 0;
    active.clear();
    ordinal = 0;
  }
  void expire(uint32_t campaign) {
    auto& state = states[campaign];
    if (--state.clicks) return;
    charge(campaign);
    active[state.position] = active.back();
    states[active.back()].position = state.position;
    active.pop_back();
  }
  void add(uint32_t campaign) {
    auto& state = states[campaign];
    if (state.clicks++ == 0) {
      state.first_session = ordinal;
      state.position = active.size();
      active.push_back(campaign);
    }
  }
};

void summarize(Bucket& bucket, const fs::path& dir, size_t part,
               const std::vector<Text>& campaigns) {
  auto path = [&](const char* name) {
    return dir / (name + std::to_string(part) + ".csv");
  };
  Sink names(path("names")), sessions(path("sessions")),
      purchases(path("purchases")), assists(path("assists"));
  for (auto& name : bucket.names) names.label(name, '\n');
  names.close();
  auto& events = bucket.events;
  auto& clicks = bucket.clicks;
  std::sort(events.begin(), events.end(), [](auto& a, auto& b) {
    return std::tie(a.user, a.ts) < std::tie(b.user, b.ts);
  });
  std::sort(clicks.begin(), clicks.end(), [](auto& a, auto& b) {
    if (a.user != b.user) return a.user < b.user;
    if (a.ts != b.ts) return a.ts < b.ts;
    return a.id > b.id;
  });
  // Copies have identical parsed fields, hence sort next to one another.
  clicks.erase(std::unique(clicks.begin(), clicks.end(),
                           [](auto& a, auto& b) { return a.id == b.id; }),
               clicks.end());
  AssistWindow window(campaigns.size());
  size_t at = 0, right = 0;
  while (at < events.size()) {
    uint32_t user = events[at].user;
    while (right < clicks.size() && clicks[right].user < user) ++right;
    size_t left = right;
    while (at < events.size() && events[at].user == user) {
      size_t first = at;
      Time start = events[at].ts, low = start - 7 * day_ms;
      int day = int(start / day_ms - (start % day_ms < 0));
      if (day != window.day) {
        window.close_day();
        window.day = day;
      }
      while (left < right && clicks[left].ts < low)
        window.expire(clicks[left++].campaign);
      // Skip clicks that fell entirely between two sessions.
      while (right < clicks.size() && clicks[right].user == user &&
             clicks[right].ts < low)
        left = ++right;
      while (right < clicks.size() && clicks[right].user == user &&
             clicks[right].ts <= start)
        window.add(clicks[right++].campaign);
      uint32_t credit = left < right ? clicks[right - 1].campaign : 0;
      int64_t revenue = 0;
      do {
        if (auto idx = events[at].purchase) {
          auto p = bucket.purchases[idx];
          purchases.number(user, ',');
          purchases.number(day, ',');
          purchases.label(p.category, ',');
          purchases.number(p.price, '\n');
          revenue += p.price;
        }
        ++at;
      } while (at < events.size() && events[at].user == user &&
               events[at].ts - events[at - 1].ts <= 1800000);
      sessions.number(user, ',');
      sessions.number(day, ',');
      sessions.number(at - first, ',');
      sessions.number(revenue, ',');
      sessions.label(campaigns[credit], '\n');
      ++window.ordinal;
    }
    window.finish_person();
  }
  for (const auto& [day, counts] : window.totals) {
    for (const auto& [campaign, count] : counts) {
      assists.label(campaigns[campaign], ',');
      assists.number(day, ',');
      assists.number(count, '\n');
    }
  }
  sessions.close();
  purchases.close();
  assists.close();
}
